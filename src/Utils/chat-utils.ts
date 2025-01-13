import { Boom } from '@hapi/boom'
import { AxiosRequestConfig } from 'axios'
import type { Logger } from 'pino'
import { proto } from '../../WAProto'
import { BaileysEventEmitter, Chat, ChatModification, ChatMutation, ChatUpdate, Contact, InitialAppStateSyncOptions, LastMessageList, LTHashState, WAPatchCreate, WAPatchName } from '../Types'
import { ChatLabelAssociation, LabelAssociationType, MessageLabelAssociation } from '../Types/LabelAssociation'
import { BinaryNode, getBinaryNodeChild, getBinaryNodeChildren, isJidGroup, jidNormalizedUser } from '../WABinary'
import { aesDecrypt, aesEncrypt, hkdf, hmacSign } from './crypto'
import { toNumber } from './generics'
import { LT_HASH_ANTI_TAMPERING } from './lt-hash'
import { downloadContentFromMessage, } from './messages-media'

type FetchAppStateSyncKey = (keyId: string) => Promise<proto.Message.IAppStateSyncKeyData | null | undefined>

export type ChatMutationMap = { [index: string]: ChatMutation }

const mutationKeys = (keydata: Uint8Array) => {
	const expanded = hkdf(keydata, 160, { info: 'WhatsApp Mutation Keys' })
	return {
		indexKey: expanded.slice(0, 32),
		valueEncryptionKey: expanded.slice(32, 64),
		valueMacKey: expanded.slice(64, 96),
		snapshotMacKey: expanded.slice(96, 128),
		patchMacKey: expanded.slice(128, 160)
	}
}

const generateMac = (operation: proto.SyncdMutation.SyncdOperation, data: Buffer, keyId: Uint8Array | string, key: Buffer) => {
	const getKeyData = () => {
		let r: number
		switch (operation) {
		case proto.SyncdMutation.SyncdOperation.SET:
			r = 0x01
			break
		case proto.SyncdMutation.SyncdOperation.REMOVE:
			r = 0x02
			break
		}

		const buff = Buffer.from([r])
		return Buffer.concat([buff, Buffer.from(keyId as string, 'base64')])
	}

	const keyData = getKeyData()

	const last = Buffer.alloc(8) // 8 bytes
	last.set([keyData.length], last.length - 1)

	const total = Buffer.concat([keyData, data, last])
	const hmac = hmacSign(total, key, 'sha512')

	return hmac.slice(0, 32)
}

const to64BitNetworkOrder = (e: number) => {
	const buff = Buffer.alloc(8)
	buff.writeUint32BE(e, 4)
	return buff
}

type Mac = { indexMac: Uint8Array, valueMac: Uint8Array, operation: proto.SyncdMutation.SyncdOperation }

const makeLtHashGenerator = ({ indexValueMap, hash }: Pick<LTHashState, 'hash' | 'indexValueMap'>) => {
	indexValueMap = { ...indexValueMap }
	const addBuffs: ArrayBuffer[] = []
	const subBuffs: ArrayBuffer[] = []

	return {
		mix: ({ indexMac, valueMac, operation }: Mac) => {
			const indexMacBase64 = Buffer.from(indexMac).toString('base64')
			const prevOp = indexValueMap[indexMacBase64]
			if(operation === proto.SyncdMutation.SyncdOperation.REMOVE) {
				if(!prevOp) {
					throw new Boom('tried remove, but no previous op', { data: { indexMac, valueMac } })
				}

				// remove from index value mac, since this mutation is erased
				delete indexValueMap[indexMacBase64]
			} else {
				addBuffs.push(new Uint8Array(valueMac).buffer)
				// add this index into the history map
				indexValueMap[indexMacBase64] = { valueMac }
			}

			if(prevOp) {
				subBuffs.push(new Uint8Array(prevOp.valueMac).buffer)
			}
		},
		finish: () => {
			const hashArrayBuffer = new Uint8Array(hash).buffer
			const result = LT_HASH_ANTI_TAMPERING.subtractThenAdd(hashArrayBuffer, addBuffs, subBuffs)
			const buffer = Buffer.from(result)

			return {
				hash: buffer,
				indexValueMap
			}
		}
	}
}

const generateSnapshotMac = (lthash: Uint8Array, version: number, name: WAPatchName, key: Buffer) => {
	const total = Buffer.concat([
		lthash,
		to64BitNetworkOrder(version),
		Buffer.from(name, 'utf-8')
	])
	return hmacSign(total, key, 'sha256')
}

const generatePatchMac = (snapshotMac: Uint8Array, valueMacs: Uint8Array[], version: number, type: WAPatchName, key: Buffer) => {
	const total = Buffer.concat([
		snapshotMac,
		...valueMacs,
		to64BitNetworkOrder(version),
		Buffer.from(type, 'utf-8')
	])
	return hmacSign(total, key)
}

export const newLTHashState = (): LTHashState => ({ version: 0, hash: Buffer.alloc(128), indexValueMap: {} })

export const encodeSyncdPatch = async(
	{ type, index, syncAction, apiVersion, operation }: WAPatchCreate,
	myAppStateKeyId: string,
	state: LTHashState,
	getAppStateSyncKey: FetchAppStateSyncKey
) => {
	const key = !!myAppStateKeyId ? await getAppStateSyncKey(myAppStateKeyId) : undefined
	if(!key) {
		throw new Boom(`myAppStateKey ("${myAppStateKeyId}") not present`, { statusCode: 404 })
	}

	const encKeyId = Buffer.from(myAppStateKeyId, 'base64')

	state = { ...state, indexValueMap: { ...state.indexValueMap } }

	const indexBuffer = Buffer.from(JSON.stringify(index))
	const dataProto = proto.SyncActionData.fromObject({
		index: indexBuffer,
		value: syncAction,
		padding: new Uint8Array(0),
		version: apiVersion
	})
	const encoded = proto.SyncActionData.encode(dataProto).finish()

	const keyValue = mutationKeys(key.keyData!)

	const encValue = aesEncrypt(encoded, keyValue.valueEncryptionKey)
	const valueMac = generateMac(operation, encValue, encKeyId, keyValue.valueMacKey)
	const indexMac = hmacSign(indexBuffer, keyValue.indexKey)

	// update LT hash
	const generator = makeLtHashGenerator(state)
	generator.mix({ indexMac, valueMac, operation })
	Object.assign(state, generator.finish())

	state.version += 1

	const snapshotMac = generateSnapshotMac(state.hash, state.version, type, keyValue.snapshotMacKey)

	const patch: proto.ISyncdPatch = {
		patchMac: generatePatchMac(snapshotMac, [valueMac], state.version, type, keyValue.patchMacKey),
		snapshotMac: snapshotMac,
		keyId: { id: encKeyId },
		mutations: [
			{
				operation: operation,
				record: {
					index: {
						blob: indexMac
					},
					value: {
						blob: Buffer.concat([encValue, valueMac])
					},
					keyId: { id: encKeyId }
				}
			}
		]
	}

	const base64Index = indexMac.toString('base64')
	state.indexValueMap[base64Index] = { valueMac }

	return { patch, state }
}

export const decodeSyncdMutations = async(
	msgMutations: (proto.ISyncdMutation | proto.ISyncdRecord)[],
	initialState: LTHashState,
	getAppStateSyncKey: FetchAppStateSyncKey,
	onMutation: (mutation: ChatMutation) => void,
	validateMacs: boolean
) => {
	const ltGenerator = makeLtHashGenerator(initialState)
	// indexKey used to HMAC sign record.index.blob
	// valueEncryptionKey used to AES-256-CBC encrypt record.value.blob[0:-32]
	// the remaining record.value.blob[0:-32] is the mac, it the HMAC sign of key.keyId + decoded proto data + length of bytes in keyId
	for(const msgMutation of msgMutations) {
		// if it's a syncdmutation, get the operation property
		// otherwise, if it's only a record -- it'll be a SET mutation
		const operation = 'operation' in msgMutation ? msgMutation.operation : proto.SyncdMutation.SyncdOperation.SET
		const record = ('record' in msgMutation && !!msgMutation.record) ? msgMutation.record : msgMutation as proto.ISyncdRecord

		const key = await getKey(record.keyId!.id!)
		const content = Buffer.from(record.value!.blob!)
		const encContent = content.slice(0, -32)
		const ogValueMac = content.slice(-32)
		if(validateMacs) {
			const contentHmac = generateMac(operation!, encContent, record.keyId!.id!, key.valueMacKey)
			if(Buffer.compare(contentHmac, ogValueMac) !== 0) {
				throw new Boom('HMAC content verification failed')
			}
		}

		const result = aesDecrypt(encContent, key.valueEncryptionKey)
		const syncAction = proto.SyncActionData.decode(result)

		if(validateMacs) {
			const hmac = hmacSign(syncAction.index, key.indexKey)
			if(Buffer.compare(hmac, record.index!.blob!) !== 0) {
				throw new Boom('HMAC index verification failed')
			}
		}

		const indexStr = Buffer.from(syncAction.index).toString()
		onMutation({ syncAction, index: JSON.parse(indexStr) })

		ltGenerator.mix({
			indexMac: record.index!.blob!,
			valueMac: ogValueMac,
			operation: operation!
		})
	}

	return ltGenerator.finish()

	async function getKey(keyId: Uint8Array) {
		const base64Key = Buffer.from(keyId).toString('base64')
		const keyEnc = await getAppStateSyncKey(base64Key)
		if(!keyEnc) {
			throw new Boom(`failed to find key "${base64Key}" to decode mutation`, { statusCode: 404, data: { msgMutations } })
		}

		return mutationKeys(keyEnc.keyData!)
	}
}

export const decodeSyncdPatch = async(
	msg: proto.ISyncdPatch,
	name: WAPatchName,
	initialState: LTHashState,
	getAppStateSyncKey: FetchAppStateSyncKey,
	onMutation: (mutation: ChatMutation) => void,
	validateMacs: boolean
) => {
	if(validateMacs) {
		const base64Key = Buffer.from(msg.keyId!.id!).toString('base64')
		const mainKeyObj = await getAppStateSyncKey(base64Key)
		if(!mainKeyObj) {
			throw new Boom(`failed to find key "${base64Key}" to decode patch`, { statusCode: 404, data: { msg } })
		}

		const mainKey = mutationKeys(mainKeyObj.keyData!)
		const mutationmacs = msg.mutations!.map(mutation => mutation.record!.value!.blob!.slice(-32))

		const patchMac = generatePatchMac(msg.snapshotMac!, mutationmacs, toNumber(msg.version!.version), name, mainKey.patchMacKey)
		if(Buffer.compare(patchMac, msg.patchMac!) !== 0) {
			throw new Boom('Invalid patch mac')
		}
	}

	const result = await decodeSyncdMutations(msg.mutations!, initialState, getAppStateSyncKey, onMutation, validateMacs)
	return result
}

export const extractSyncdPatches = async(
	result: BinaryNode,
	options: AxiosRequestConfig<{}>
) => {
	const syncNode = getBinaryNodeChild(result, 'sync')
	const collectionNodes = getBinaryNodeChildren(syncNode, 'collection')

	const final = {} as { [T in WAPatchName]: { patches: proto.ISyncdPatch[], hasMorePatches: boolean, snapshot?: proto.ISyncdSnapshot } }
	await Promise.all(
		collectionNodes.map(
			async collectionNode => {
				const patchesNode = getBinaryNodeChild(collectionNode, 'patches')

				const patches = getBinaryNodeChildren(patchesNode || collectionNode, 'patch')
				const snapshotNode = getBinaryNodeChild(collectionNode, 'snapshot')

				const syncds: proto.ISyncdPatch[] = []
				const name = collectionNode.attrs.name as WAPatchName

				const hasMorePatches = collectionNode.attrs.has_more_patches === 'true'

				let snapshot: proto.ISyncdSnapshot | undefined = undefined
				if(snapshotNode && !!snapshotNode.content) {
					if(!Buffer.isBuffer(snapshotNode)) {
						snapshotNode.content = Buffer.from(Object.values(snapshotNode.content))
					}

					const blobRef = proto.ExternalBlobReference.decode(
						snapshotNode.content as Buffer
					)
					const data = await downloadExternalBlob(blobRef, options)
					snapshot = proto.SyncdSnapshot.decode(data)
				}

				for(let { content } of patches) {
					if(content) {
						if(!Buffer.isBuffer(content)) {
							content = Buffer.from(Object.values(content))
						}

						const syncd = proto.SyncdPatch.decode(content as Uint8Array)
						if(!syncd.version) {
							syncd.version = { version: +collectionNode.attrs.version + 1 }
						}

						syncds.push(syncd)
					}
				}

				final[name] = { patches: syncds, hasMorePatches, snapshot }
			}
		)
	)

	return final
}


export const downloadExternalBlob = async(
	blob: proto.IExternalBlobReference,
	options: AxiosRequestConfig<{}>
) => {
	const stream = await downloadContentFromMessage(blob, 'md-app-state', { options })
	const bufferArray: Buffer[] = []
	for await (const chunk of stream) {
		bufferArray.push(chunk)
	}

	return Buffer.concat(bufferArray)
}

export const downloadExternalPatch = async(
	blob: proto.IExternalBlobReference,
	options: AxiosRequestConfig<{}>
) => {
	const buffer = await downloadExternalBlob(blob, options)
	const syncData = proto.SyncdMutations.decode(buffer)
	return syncData
}

export const decodeSyncdSnapshot = async(
	name: WAPatchName,
	snapshot: proto.ISyncdSnapshot,
	getAppStateSyncKey: FetchAppStateSyncKey,
	minimumVersionNumber: number | undefined,
	validateMacs = true
) => {
	const newState = newLTHashState()
	newState.version = toNumber(snapshot.version!.version)

	const mutationMap: ChatMutationMap = {}
	const areMutationsRequired = typeof minimumVersionNumber === 'undefined'
		|| newState.version > minimumVersionNumber

	const { hash, indexValueMap } = await decodeSyncdMutations(
		snapshot.records!,
		newState,
		getAppStateSyncKey,
		areMutationsRequired
			? (mutation) => {
				const index = mutation.syncAction.index?.toString()
				mutationMap[index!] = mutation
			}
			: () => { },
		validateMacs
	)
	newState.hash = hash
	newState.indexValueMap = indexValueMap

	if(validateMacs) {
		const base64Key = Buffer.from(snapshot.keyId!.id!).toString('base64')
		const keyEnc = await getAppStateSyncKey(base64Key)
		if(!keyEnc) {
			throw new Boom(`failed to find key "${base64Key}" to decode mutation`)
		}

		const result = mutationKeys(keyEnc.keyData!)
		const computedSnapshotMac = generateSnapshotMac(newState.hash, newState.version, name, result.snapshotMacKey)
		if(Buffer.compare(snapshot.mac!, computedSnapshotMac) !== 0) {
			throw new Boom(`failed to verify LTHash at ${newState.version} of ${name} from snapshot`)
		}
	}

	return {
		state: newState,
		mutationMap
	}
}

export const decodePatches = async(
	name: WAPatchName,
	syncds: proto.ISyncdPatch[],
	initial: LTHashState,
	getAppStateSyncKey: FetchAppStateSyncKey,
	options: AxiosRequestConfig<{}>,
	minimumVersionNumber?: number,
	logger?: Logger,
	validateMacs = true
) => {
	const newState: LTHashState = {
		...initial,
		indexValueMap: { ...initial.indexValueMap }
	}

	const mutationMap: ChatMutationMap = {}

	for(const syncd of syncds) {
		const { version, keyId, snapshotMac } = syncd
		if(syncd.externalMutations) {
			logger?.trace({ name, version }, 'downloading external patch')
			const ref = await downloadExternalPatch(syncd.externalMutations, options)
			logger?.debug({ name, version, mutations: ref.mutations.length }, 'downloaded external patch')
			syncd.mutations?.push(...ref.mutations)
		}

		const patchVersion = toNumber(version!.version)

		newState.version = patchVersion
		const shouldMutate = typeof minimumVersionNumber === 'undefined' || patchVersion > minimumVersionNumber

		const decodeResult = await decodeSyncdPatch(
			syncd,
			name,
			newState,
			getAppStateSyncKey,
			shouldMutate
				? mutation => {
					const index = mutation.syncAction.index?.toString()
					mutationMap[index!] = mutation
				}
				: (() => { }),
			true
		)

		newState.hash = decodeResult.hash
		newState.indexValueMap = decodeResult.indexValueMap

		if(validateMacs) {
			const base64Key = Buffer.from(keyId!.id!).toString('base64')
			const keyEnc = await getAppStateSyncKey(base64Key)
			if(!keyEnc) {
				throw new Boom(`failed to find key "${base64Key}" to decode mutation`)
			}

			const result = mutationKeys(keyEnc.keyData!)
			const computedSnapshotMac = generateSnapshotMac(newState.hash, newState.version, name, result.snapshotMacKey)
			if(Buffer.compare(snapshotMac!, computedSnapshotMac) !== 0) {
				throw new Boom(`failed to verify LTHash at ${newState.version} of ${name}`)
			}
		}

		// clear memory used up by the mutations
		syncd.mutations = []
	}

	return { state: newState, mutationMap }
}

export const chatModificationToAppPatch = (
	mod: ChatModification,
	jid: string
) => {
	const OP = proto.SyncdMutation.SyncdOperation
	const getMessageRange = (lastMessages: LastMessageList) => {
		let messageRange: proto.SyncActionValue.ISyncActionMessageRange
		if(Array.isArray(lastMessages)) {
			const lastMsg = lastMessages[lastMessages.length - 1]
			messageRange = {
				lastMessageTimestamp: lastMsg?.messageTimestamp,
				messages: lastMessages?.length ? lastMessages.map(
					m => {
						if(!m.key?.id || !m.key?.remoteJid) {
							throw new Boom('Incomplete key', { statusCode: 400, data: m })
						}

						if(isJidGroup(m.key.remoteJid) && !m.key.fromMe && !m.key.participant) {
							throw new Boom('Expected not from me message to have participant', { statusCode: 400, data: m })
						}

						if(!m.messageTimestamp || !toNumber(m.messageTimestamp)) {
							throw new Boom('Missing timestamp in last message list', { statusCode: 400, data: m })
						}

						if(m.key.participant) {
							m.key.participant = jidNormalizedUser(m.key.participant)
						}

						return m
					}
				) : undefined
			}
		} else {
			messageRange = lastMessages
		}

		return messageRange
	}

	let patch: WAPatchCreate
	if('mute' in mod) {
		patch = {
			syncAction: {
				muteAction: {
					muted: !!mod.mute,
					muteEndTimestamp: mod.mute || undefined
				}
			},
			index: ['mute', jid],
			type: 'regular_high',
			apiVersion: 2,
			operation: OP.SET
		}
	} else if('archive' in mod) {
		patch = {
			syncAction: {
				archiveChatAction: {
					archived: !!mod.archive,
					messageRange: getMessageRange(mod.lastMessages)
				}
			},
			index: ['archive', jid],
			type: 'regular_low',
			apiVersion: 3,
			operation: OP.SET
		}
	} else if('markRead' in mod) {
		patch = {
			syncAction: {
				markChatAsReadAction: {
					read: mod.markRead,
					messageRange: getMessageRange(mod.lastMessages)
				}
			},
			index: ['markChatAsRead', jid],
			type: 'regular_low',
			apiVersion: 3,
			operation: OP.SET
		}
	} else if('deleteForMe' in mod) {
		const { timestamp, key, deleteMedia } = mod.deleteForMe
		patch = {
			syncAction: {
				deleteMessageForMeAction: {
					deleteMedia,
					messageTimestamp: timestamp
				}
			},
			index: ['deleteMessageForMe', jid, key.id!, key.fromMe ? '1' : '0', '0'],
			type: 'regular_high',
			apiVersion: 3,
			operation: OP.SET
		}
	} else if('clear' in mod) {
		patch = {
			syncAction: {
				clearChatAction: {} // add message range later
			},
			index: ['clearChat', jid, '1' /*the option here is 0 when keep starred messages is enabled*/, '0'],
			type: 'regular_high',
			apiVersion: 6,
			operation: OP.SET
		}
	} else if('pin' in mod) {
		patch = {
			syncAction: {
				pinAction: {
					pinned: !!mod.pin
				}
			},
			index: ['pin_v1', jid],
			type: 'regular_low',
			apiVersion: 5,
			operation: OP.SET
		}
	} else if('contact' in mod) {
		patch = {
			syncAction: {
				contactAction: mod.contact || {}
			},
			index: ['contact', jid],
			type: 'critical_unblock_low',
			apiVersion: 2,
			operation: mod.contact ? OP.SET : OP.REMOVE
		}
	} else if('star' in mod) {
		const key = mod.star.messages[0]
		patch = {
			syncAction: {
				starAction: {
					starred: !!mod.star.star
				}
			},
			index: ['star', jid, key.id, key.fromMe ? '1' : '0', '0'],
			type: 'regular_low',
			apiVersion: 2,
			operation: OP.SET
		}
	} else if('delete' in mod) {
		patch = {
			syncAction: {
				deleteChatAction: {
					messageRange: getMessageRange(mod.lastMessages),
				}
			},
			index: ['deleteChat', jid, '1'],
			type: 'regular_high',
			apiVersion: 6,
			operation: OP.SET
		}
	} else if('pushNameSetting' in mod) {
		patch = {
			syncAction: {
				pushNameSetting: {
					name: mod.pushNameSetting
				}
			},
			index: ['setting_pushName'],
			type: 'critical_block',
			apiVersion: 1,
			operation: OP.SET,
		}
	} else if('addLabel' in mod) {
		patch = {
			syncAction: {
				labelEditAction: {
					name: mod.addLabel.name,
					color: mod.addLabel.color,
					predefinedId : mod.addLabel.predefinedId,
					deleted: mod.addLabel.deleted
				}
			},
			index: ['label_edit', mod.addLabel.id],
			type: 'regular',
			apiVersion: 3,
			operation: OP.SET,
		}
	} else if('addChatLabel' in mod) {
		patch = {
			syncAction: {
				labelAssociationAction: {
					labeled: true,
				}
			},
			index: [LabelAssociationType.Chat, mod.addChatLabel.labelId, jid],
			type: 'regular',
			apiVersion: 3,
			operation: OP.SET,
		}
	} else if('removeChatLabel' in mod) {
		patch = {
			syncAction: {
				labelAssociationAction: {
					labeled: false,
				}
			},
			index: [LabelAssociationType.Chat, mod.removeChatLabel.labelId, jid],
			type: 'regular',
			apiVersion: 3,
			operation: OP.SET,
		}
	} else if('addMessageLabel' in mod) {
		patch = {
			syncAction: {
				labelAssociationAction: {
					labeled: true,
				}
			},
			index: [
				LabelAssociationType.Message,
				mod.addMessageLabel.labelId,
				jid,
				mod.addMessageLabel.messageId,
				'0',
				'0'
			],
			type: 'regular',
			apiVersion: 3,
			operation: OP.SET,
		}
	} else if('removeMessageLabel' in mod) {
		patch = {
			syncAction: {
				labelAssociationAction: {
					labeled: false,
				}
			},
			index: [
				LabelAssociationType.Message,
				mod.removeMessageLabel.labelId,
				jid,
				mod.removeMessageLabel.messageId,
				'0',
				'0'
			],
			type: 'regular',
			apiVersion: 3,
			operation: OP.SET,
		}
	} else {
		throw new Boom('not supported')
	}

	patch.syncAction.timestamp = Date.now()

	return patch
}

export const processSyncAction = (
	syncAction: ChatMutation,
	ev: BaileysEventEmitter,
	me: Contact,
	initialSyncOpts?: InitialAppStateSyncOptions,
	logger?: Logger,
) => {
	const isInitialSync = !!initialSyncOpts
	const accountSettings = initialSyncOpts?.accountSettings

	logger?.trace({ syncAction, initialSync: !!initialSyncOpts }, 'processing sync action')

	const {
		syncAction: { value: action },
		index: [type, id, msgId, fromMe]
	} = syncAction

	if(action?.muteAction) {
		ev.emit(
			'chats.update',
			[
				{
					id,
					muteEndTime: action.muteAction?.muted
						? toNumber(action.muteAction.muteEndTimestamp)
						: null,
					conditional: getChatUpdateConditional(id, undefined)
				}
			]
		)
	} else if(action?.archiveChatAction || type === 'archive' || type === 'unarchive') {
		// okay so we've to do some annoying computation here
		// when we're initially syncing the app state
		// there are a few cases we need to handle
		// 1. if the account unarchiveChats setting is true
		//   a. if the chat is archived, and no further messages have been received -- simple, keep archived
		//   b. if the chat was archived, and the user received messages from the other person afterwards
		//		then the chat should be marked unarchved --
		//		we compare the timestamp of latest message from the other person to determine this
		// 2. if the account unarchiveChats setting is false -- then it doesn't matter,
		//	it'll always take an app state action to mark in unarchived -- which we'll get anyway
		const archiveAction = action?.archiveChatAction
		const isArchived = archiveAction
			? archiveAction.archived
			: type === 'archive'
		// // basically we don't need to fire an "archive" update if the chat is being marked unarchvied
		// // this only applies for the initial sync
		// if(isInitialSync && !isArchived) {
		// 	isArchived = false
		// }

		const msgRange = !accountSettings?.unarchiveChats ? undefined : archiveAction?.messageRange
		// logger?.debug({ chat: id, syncAction }, 'message range archive')

		ev.emit('chats.update', [{
			id,
			archived: isArchived,
			conditional: getChatUpdateConditional(id, msgRange)
		}])
	} else if(action?.markChatAsReadAction) {
		const markReadAction = action.markChatAsReadAction
		// basically we don't need to fire an "read" update if the chat is being marked as read
		// because the chat is read by default
		// this only applies for the initial sync
		const isNullUpdate = isInitialSync && markReadAction.read

		ev.emit('chats.update', [{
			id,
			unreadCount: isNullUpdate ? null : !!markReadAction?.read ? 0 : -1,
			conditional: getChatUpdateConditional(id, markReadAction?.messageRange)
		}])
	} else if(action?.deleteMessageForMeAction || type === 'deleteMessageForMe') {
		ev.emit('messages.delete', {
			keys: [
				{
					remoteJid: id,
					id: msgId,
					fromMe: fromMe === '1'
				}
			]
		})
	} else if(action?.contactAction) {
		ev.emit('contacts.upsert', [{ id, name: action.contactAction.fullName! }])
	} else if(action?.pushNameSetting) {
		const name = action?.pushNameSetting?.name
		if(name && me?.name !== name) {
			ev.emit('creds.update', { me: { ...me, name } })
		}
	} else if(action?.pinAction) {
		ev.emit('chats.update', [{
			id,
			pinned: action.pinAction?.pinned ? toNumber(action.timestamp) : null,
			conditional: getChatUpdateConditional(id, undefined)
		}])
	} else if(action?.unarchiveChatsSetting) {
		const unarchiveChats = !!action.unarchiveChatsSetting.unarchiveChats
		ev.emit('creds.update', { accountSettings: { unarchiveChats } })

		logger?.info(`archive setting updated => '${action.unarchiveChatsSetting.unarchiveChats}'`)
		if(accountSettings) {
			accountSettings.unarchiveChats = unarchiveChats
		}
	} else if(action?.starAction || type === 'star') {
		let starred = action?.starAction?.starred
		if(typeof starred !== 'boolean') {
			starred = syncAction.index[syncAction.index.length - 1] === '1'
		}

		ev.emit('messages.update', [
			{
				key: { remoteJid: id, id: msgId, fromMe: fromMe === '1' },
				update: { starred }
			}
		])
	} else if(action?.deleteChatAction || type === 'deleteChat') {
		if(!isInitialSync) {
			ev.emit('chats.delete', [id])
		}
	} else if(action?.labelEditAction) {
		const { name, color, deleted, predefinedId } = action.labelEditAction

		ev.emit('labels.edit', {
			id,
			name: name!,
			color: color!,
			deleted: deleted!,
			predefinedId: predefinedId ? String(predefinedId) : undefined
		})
	} else if(action?.labelAssociationAction) {
		ev.emit('labels.association', {
			type: action.labelAssociationAction.labeled
				? 'add'
				: 'remove',
			association: type === LabelAssociationType.Chat
				? {
					type: LabelAssociationType.Chat,
					chatId: syncAction.index[2],
					labelId: syncAction.index[1]
				} as ChatLabelAssociation
				: {
					type: LabelAssociationType.Message,
					chatId: syncAction.index[2],
					messageId: syncAction.index[3],
					labelId: syncAction.index[1]
				} as MessageLabelAssociation
		})
	} else {
		logger?.debug({ syncAction, id }, 'unprocessable update')
	}

	function getChatUpdateConditional(id: string, msgRange: proto.SyncActionValue.ISyncActionMessageRange | null | undefined): ChatUpdate['conditional'] {
		return isInitialSync
			? (data) => {
				const chat = data.historySets.chats[id] || data.chatUpserts[id]
				if(chat) {
					return msgRange ? isValidPatchBasedOnMessageRange(chat, msgRange) : true
				}
			}
			: undefined
	}

	function isValidPatchBasedOnMessageRange(chat: Chat, msgRange: proto.SyncActionValue.ISyncActionMessageRange | null | undefined) {
		  const lastMsgTimestamp = Number(msgRange?.lastMessageTimestamp || msgRange?.lastSystemMessageTimestamp || 0)
		  const chatLastMsgTimestamp = Number(chat?.lastMessageRecvTimestamp || 0)
		  return lastMsgTimestamp >= chatLastMsgTimestamp
	}
}
