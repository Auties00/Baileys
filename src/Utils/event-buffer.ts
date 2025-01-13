import EventEmitter from 'events'
import { proto } from '../../WAProto'
import { BaileysEvent, BaileysEventEmitter, BaileysEventMap, BufferedEventData, Chat, ChatUpdate, Contact, WAMessage, WAMessageStatus } from '../Types'
import { trimUndefined } from './generics'
import { ILogger } from './logger'
import { updateMessageWithReaction, updateMessageWithReceipt } from './messages'
import { isRealMessage, shouldIncrementChatUnread } from './process-message'

const BUFFERABLE_EVENT = [
	'messaging-history.set',
	'chats.upsert',
	'chats.update',
	'chats.delete',
	'contacts.upsert',
	'contacts.update',
	'messages.upsert',
	'messages.update',
	'messages.delete',
	'messages.reaction',
	'message-receipt.update',
	'groups.update',
] as const

type BufferableEvent = typeof BUFFERABLE_EVENT[number]

/**
 * A map that contains a list of all events that have been triggered
 *
 * Note, this can contain different type of events
 * this can make processing events extremely efficient -- since everything
 * can be done in a single transaction
 */
type BaileysEventData = Partial<BaileysEventMap>

const BUFFERABLE_EVENT_SET = new Set<BaileysEvent>(BUFFERABLE_EVENT)

type BaileysBufferableEventEmitter = BaileysEventEmitter & {
	/** Use to process events in a batch */
	process(handler: (events: BaileysEventData) => void | Promise<void>): (() => void)
	/**
	 * starts buffering events, call flush() to release them
	 * */
	buffer(): void
	/** buffers all events till the promise completes */
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	createBufferedFunction<A extends any[], T>(work: (...args: A) => Promise<T>): ((...args: A) => Promise<T>)
	/**
	 * flushes all buffered events
	 * @param force if true, will flush all data regardless of any pending buffers
	 * @returns returns true if the flush actually happened, otherwise false
	 */
	flush(force?: boolean): boolean
	/** is there an ongoing buffer */
	isBuffering(): boolean
}

/**
 * The event buffer logically consolidates different events into a single event
 * making the data processing more efficient.
 * @param ev the baileys event emitter
 */
export const makeEventBuffer = (logger: ILogger): BaileysBufferableEventEmitter => {
	const ev = new EventEmitter()
	const historyCache = new Set<string>()

	let data = makeBufferData()
	let buffersInProgress = 0

	// take the generic event and fire it as a baileys event
	ev.on('event', (map: BaileysEventData) => {
		for(const event in map) {
			ev.emit(event, map[event])
		}
	})

	function buffer() {
		buffersInProgress += 1
	}

	function flush(force = false) {
		// no buffer going on
		if(!buffersInProgress) {
			return false
		}

		if(!force) {
			// reduce the number of buffers in progress
			buffersInProgress -= 1
			// if there are still some buffers going on
			// then we don't flush now
			if(buffersInProgress) {
				return false
			}
		}

		const newData = makeBufferData()
		const chatUpdates = Object.values(data.chatUpdates)
		// gather the remaining conditional events so we re-queue them
		let conditionalChatUpdatesLeft = 0
		for(const update of chatUpdates) {
			if(update.conditional) {
				conditionalChatUpdatesLeft += 1
				newData.chatUpdates[update.id!] = update
				delete data.chatUpdates[update.id!]
			}
		}

		const consolidatedData = consolidateEvents(data)
		if(Object.keys(consolidatedData).length) {
			ev.emit('event', consolidatedData)
		}

		data = newData

		logger.trace(
			{ conditionalChatUpdatesLeft },
			'released buffered events'
		)

		return true
	}

	return {
		process(handler) {
			const listener = (map: BaileysEventData) => {
				handler(map)
			}

			ev.on('event', listener)
			return () => {
				ev.off('event', listener)
			}
		},
		emit<T extends BaileysEvent>(event: BaileysEvent, evData: BaileysEventMap[T]) {
			if(buffersInProgress && BUFFERABLE_EVENT_SET.has(event)) {
				append(data, historyCache, event as BufferableEvent, evData, logger)
				return true
			}

			return ev.emit('event', { [event]: evData })
		},
		isBuffering() {
			return buffersInProgress > 0
		},
		buffer,
		flush,
		createBufferedFunction(work) {
			return async(...args) => {
				buffer()
				try {
					const result = await work(...args)
					return result
				} finally {
					flush()
				}
			}
		},
		on: (...args) => ev.on(...args),
		off: (...args) => ev.off(...args),
		removeAllListeners: (...args) => ev.removeAllListeners(...args),
	}
}

const makeBufferData = (): BufferedEventData => {
	return {
		historySets: {
			chats: { },
			messages: { },
			contacts: { },
			isLatest: false,
			empty: true
		},
		chatUpserts: { },
		chatUpdates: { },
		chatDeletes: new Set(),
		contactUpserts: { },
		contactUpdates: { },
		messageUpserts: { },
		messageUpdates: { },
		messageReactions: { },
		messageDeletes: { },
		messageReceipts: { },
		groupUpdates: { }
	}
}

function append<E extends BufferableEvent>(
	data: BufferedEventData,
	historyCache: Set<string>,
	event: E,
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	eventData: any,
	logger: ILogger
) {
	switch (event) {
	case 'messaging-history.set':
		for(const chat of eventData.chats as Chat[]) {
			const existingChat = data.historySets.chats[chat.id]
			if(existingChat) {
				existingChat.endOfHistoryTransferType = chat.endOfHistoryTransferType
			}

			if(!existingChat && !historyCache.has(chat.id)) {
				data.historySets.chats[chat.id] = chat
				historyCache.add(chat.id)

				absorbingChatUpdate(chat)
			}
		}

		for(const contact of eventData.contacts as Contact[]) {
			const existingContact = data.historySets.contacts[contact.id]
			if(existingContact) {
				Object.assign(existingContact, trimUndefined(contact))
			} else {
				const historyContactId = `c:${contact.id}`
				const hasAnyName = contact.notify || contact.name || contact.verifiedName
				if(!historyCache.has(historyContactId) || hasAnyName) {
					data.historySets.contacts[contact.id] = contact
					historyCache.add(historyContactId)
				}
			}
		}

		for(const message of eventData.messages as WAMessage[]) {
			const key = stringifyMessageKey(message.key)
			const existingMsg = data.historySets.messages[key]
			if(!existingMsg && !historyCache.has(key)) {
				data.historySets.messages[key] = message
				historyCache.add(key)
			}
		}

		data.historySets.empty = false
		data.historySets.syncType = eventData.syncType
		data.historySets.progress = eventData.progress
		data.historySets.peerDataRequestSessionId = eventData.peerDataRequestSessionId
		data.historySets.isLatest = eventData.isLatest || data.historySets.isLatest

		break
	case 'chats.upsert':
		for(const chat of eventData as Chat[]) {
			let upsert = data.chatUpserts[chat.id]
			if(!upsert) {
				upsert = data.historySets[chat.id]
				if(upsert) {
					logger.debug({ chatId: chat.id }, 'absorbed chat upsert in chat set')
				}
			}

			if(upsert) {
				upsert = concatChats(upsert, chat)
			} else {
				upsert = chat
				data.chatUpserts[chat.id] = upsert
			}

			absorbingChatUpdate(upsert)

			if(data.chatDeletes.has(chat.id)) {
				data.chatDeletes.delete(chat.id)
			}
		}

		break
	case 'chats.update':
		for(const update of eventData as ChatUpdate[]) {
			const chatId = update.id!
			const conditionMatches = update.conditional ? update.conditional(data) : true
			if(conditionMatches) {
				delete update.conditional

				// if there is an existing upsert, merge the update into it
				const upsert = data.historySets.chats[chatId] || data.chatUpserts[chatId]
				if(upsert) {
					concatChats(upsert, update)
				} else {
					// merge the update into the existing update
					const chatUpdate = data.chatUpdates[chatId] || { }
					data.chatUpdates[chatId] = concatChats(chatUpdate, update)
				}
			} else if(conditionMatches === undefined) {
				// condition yet to be fulfilled
				data.chatUpdates[chatId] = update
			}
			// otherwise -- condition not met, update is invalid

			// if the chat has been updated
			// ignore any existing chat delete
			if(data.chatDeletes.has(chatId)) {
				data.chatDeletes.delete(chatId)
			}
		}

		break
	case 'chats.delete':
		for(const chatId of eventData as string[]) {
			if(!data.chatDeletes.has(chatId)) {
				data.chatDeletes.add(chatId)
			}

			// remove any prior updates & upserts
			if(data.chatUpdates[chatId]) {
				delete data.chatUpdates[chatId]
			}

			if(data.chatUpserts[chatId]) {
				delete data.chatUpserts[chatId]

			}

			if(data.historySets.chats[chatId]) {
				delete data.historySets.chats[chatId]
			}
		}

		break
	case 'contacts.upsert':
		for(const contact of eventData as Contact[]) {
			let upsert = data.contactUpserts[contact.id]
			if(!upsert) {
				upsert = data.historySets.contacts[contact.id]
				if(upsert) {
					logger.debug({ contactId: contact.id }, 'absorbed contact upsert in contact set')
				}
			}

			if(upsert) {
				upsert = Object.assign(upsert, trimUndefined(contact))
			} else {
				upsert = contact
				data.contactUpserts[contact.id] = upsert
			}

			if(data.contactUpdates[contact.id]) {
				upsert = Object.assign(data.contactUpdates[contact.id], trimUndefined(contact)) as Contact
				delete data.contactUpdates[contact.id]
			}
		}

		break
	case 'contacts.update':
		const contactUpdates = eventData as BaileysEventMap['contacts.update']
		for(const update of contactUpdates) {
			const id = update.id!
			// merge into prior upsert
			const upsert = data.historySets.contacts[id] || data.contactUpserts[id]
			if(upsert) {
				Object.assign(upsert, update)
			} else {
				// merge into prior update
				const contactUpdate = data.contactUpdates[id] || { }
				data.contactUpdates[id] = Object.assign(contactUpdate, update)
			}
		}

		break
	case 'messages.upsert':
		const { messages, type } = eventData as BaileysEventMap['messages.upsert']
		for(const message of messages) {
			const key = stringifyMessageKey(message.key)
			let existing = data.messageUpserts[key]?.message
			if(!existing) {
				existing = data.historySets.messages[key]
				if(existing) {
					logger.debug({ messageId: key }, 'absorbed message upsert in message set')
				}
			}

			if(existing) {
				message.messageTimestamp = existing.messageTimestamp
			}

			if(data.messageUpdates[key]) {
				logger.debug('absorbed prior message update in message upsert')
				Object.assign(message, data.messageUpdates[key].update)
				delete data.messageUpdates[key]
			}

			if(data.historySets.messages[key]) {
				data.historySets.messages[key] = message
			} else {
				data.messageUpserts[key] = {
					message,
					type: type === 'notify' || data.messageUpserts[key]?.type === 'notify'
						? 'notify'
						: type
				}
			}
		}

		break
	case 'messages.update':
		const msgUpdates = eventData as BaileysEventMap['messages.update']
		for(const { key, update } of msgUpdates) {
			const keyStr = stringifyMessageKey(key)
			const existing = data.historySets.messages[keyStr] || data.messageUpserts[keyStr]?.message
			if(existing) {
				Object.assign(existing, update)
				// if the message was received & read by us
				// the chat counter must have been incremented
				// so we need to decrement it
				if(update.status === WAMessageStatus.READ && !key.fromMe) {
					decrementChatReadCounterIfMsgDidUnread(existing)
				}
			} else {
				const msgUpdate = data.messageUpdates[keyStr] || { key, update: { } }
				Object.assign(msgUpdate.update, update)
				data.messageUpdates[keyStr] = msgUpdate
			}
		}

		break
	case 'messages.delete':
		const deleteData = eventData as BaileysEventMap['messages.delete']
		if('keys' in deleteData) {
			const { keys } = deleteData
			for(const key of keys) {
				const keyStr = stringifyMessageKey(key)
				if(!data.messageDeletes[keyStr]) {
					data.messageDeletes[keyStr] = key

				}

				if(data.messageUpserts[keyStr]) {
					delete data.messageUpserts[keyStr]
				}

				if(data.messageUpdates[keyStr]) {
					delete data.messageUpdates[keyStr]
				}
			}
		} else {
			// TODO: add support
		}

		break
	case 'messages.reaction':
		const reactions = eventData as BaileysEventMap['messages.reaction']
		for(const { key, reaction } of reactions) {
			const keyStr = stringifyMessageKey(key)
			const existing = data.messageUpserts[keyStr]
			if(existing) {
				updateMessageWithReaction(existing.message, reaction)
			} else {
				data.messageReactions[keyStr] = data.messageReactions[keyStr]
					|| { key, reactions: [] }
				updateMessageWithReaction(data.messageReactions[keyStr], reaction)
			}
		}

		break
	case 'message-receipt.update':
		const receipts = eventData as BaileysEventMap['message-receipt.update']
		for(const { key, receipt } of receipts) {
			const keyStr = stringifyMessageKey(key)
			const existing = data.messageUpserts[keyStr]
			if(existing) {
				updateMessageWithReceipt(existing.message, receipt)
			} else {
				data.messageReceipts[keyStr] = data.messageReceipts[keyStr]
					|| { key, userReceipt: [] }
				updateMessageWithReceipt(data.messageReceipts[keyStr], receipt)
			}
		}

		break
	case 'groups.update':
		const groupUpdates = eventData as BaileysEventMap['groups.update']
		for(const update of groupUpdates) {
			const id = update.id!
			const groupUpdate = data.groupUpdates[id] || { }
			if(!data.groupUpdates[id]) {
				data.groupUpdates[id] = Object.assign(groupUpdate, update)

			}
		}

		break
	default:
		throw new Error(`"${event}" cannot be buffered`)
	}

	function absorbingChatUpdate(existing: Chat) {
		const chatId = existing.id
		const update = data.chatUpdates[chatId]
		if(update) {
			const conditionMatches = update.conditional ? update.conditional(data) : true
			if(conditionMatches) {
				delete update.conditional
				logger.debug({ chatId }, 'absorbed chat update in existing chat')
				Object.assign(existing, concatChats(update as Chat, existing))
				delete data.chatUpdates[chatId]
			} else if(conditionMatches === false) {
				logger.debug({ chatId }, 'chat update condition fail, removing')
				delete data.chatUpdates[chatId]
			}
		}
	}

	function decrementChatReadCounterIfMsgDidUnread(message: WAMessage) {
		// decrement chat unread counter
		// if the message has already been marked read by us
		const chatId = message.key.remoteJid!
		const chat = data.chatUpdates[chatId] || data.chatUpserts[chatId]
		if(
			isRealMessage(message, '')
			&& shouldIncrementChatUnread(message)
			&& typeof chat?.unreadCount === 'number'
			&& chat.unreadCount > 0
		) {
			logger.debug({ chatId: chat.id }, 'decrementing chat counter')
			chat.unreadCount -= 1
			if(chat.unreadCount === 0) {
				delete chat.unreadCount
			}
		}
	}
}

function consolidateEvents(data: BufferedEventData) {
	const map: BaileysEventData = { }

	if(!data.historySets.empty) {
		map['messaging-history.set'] = {
			chats: Object.values(data.historySets.chats),
			messages: Object.values(data.historySets.messages),
			contacts: Object.values(data.historySets.contacts),
			syncType: data.historySets.syncType,
			progress: data.historySets.progress,
			isLatest: data.historySets.isLatest,
			peerDataRequestSessionId: data.historySets.peerDataRequestSessionId
		}
	}

	const chatUpsertList = Object.values(data.chatUpserts)
	if(chatUpsertList.length) {
		map['chats.upsert'] = chatUpsertList
	}

	const chatUpdateList = Object.values(data.chatUpdates)
	if(chatUpdateList.length) {
		map['chats.update'] = chatUpdateList
	}

	const chatDeleteList = Array.from(data.chatDeletes)
	if(chatDeleteList.length) {
		map['chats.delete'] = chatDeleteList
	}

	const messageUpsertList = Object.values(data.messageUpserts)
	if(messageUpsertList.length) {
		const type = messageUpsertList[0].type
		map['messages.upsert'] = {
			messages: messageUpsertList.map(m => m.message),
			type
		}
	}

	const messageUpdateList = Object.values(data.messageUpdates)
	if(messageUpdateList.length) {
		map['messages.update'] = messageUpdateList
	}

	const messageDeleteList = Object.values(data.messageDeletes)
	if(messageDeleteList.length) {
		map['messages.delete'] = { keys: messageDeleteList }
	}

	const messageReactionList = Object.values(data.messageReactions).flatMap(
		({ key, reactions }) => reactions.flatMap(reaction => ({ key, reaction }))
	)
	if(messageReactionList.length) {
		map['messages.reaction'] = messageReactionList
	}

	const messageReceiptList = Object.values(data.messageReceipts).flatMap(
		({ key, userReceipt }) => userReceipt.flatMap(receipt => ({ key, receipt }))
	)
	if(messageReceiptList.length) {
		map['message-receipt.update'] = messageReceiptList
	}

	const contactUpsertList = Object.values(data.contactUpserts)
	if(contactUpsertList.length) {
		map['contacts.upsert'] = contactUpsertList
	}

	const contactUpdateList = Object.values(data.contactUpdates)
	if(contactUpdateList.length) {
		map['contacts.update'] = contactUpdateList
	}

	const groupUpdateList = Object.values(data.groupUpdates)
	if(groupUpdateList.length) {
		map['groups.update'] = groupUpdateList
	}

	return map
}

function concatChats<C extends Partial<Chat>>(a: C, b: Partial<Chat>) {
	if(b.unreadCount === null && // neutralize unread counter
		a.unreadCount! < 0) {
		a.unreadCount = undefined
		b.unreadCount = undefined
	}

	if(typeof a.unreadCount === 'number' && typeof b.unreadCount === 'number') {
		b = { ...b }
		if(b.unreadCount! >= 0) {
			b.unreadCount = Math.max(b.unreadCount!, 0) + Math.max(a.unreadCount, 0)
		}
	}

	return Object.assign(a, b)
}

const stringifyMessageKey = (key: proto.IMessageKey) => `${key.remoteJid},${key.id},${key.fromMe ? '1' : '0'}`