import { Boom } from '@hapi/boom'
import axios from 'axios'
import { randomBytes } from 'crypto'
import { promises as fs } from 'fs'
import { type Transform } from 'stream'
import { proto } from '../../WAProto'
import { MEDIA_KEYS, URL_REGEX, WA_DEFAULT_EPHEMERAL } from '../Defaults'
import {
	AnyMediaMessageContent,
	AnyMessageContent,
	DownloadableMessage,
	MediaGenerationOptions,
	MediaType,
	MessageContentGenerationOptions,
	MessageGenerationOptions,
	MessageGenerationOptionsFromContent,
	MessageType,
	MessageUserReceipt,
	WAMediaUpload,
	WAMessage,
	WAMessageContent,
	WAMessageStatus,
	WAProto,
	WATextMessage,
} from '../Types'
import { isJidGroup, isJidStatusBroadcast, jidNormalizedUser } from '../WABinary'
import { sha256 } from './crypto'
import { generateMessageID, getKeyAuthor, unixTimestampSeconds } from './generics'
import { ILogger } from './logger'
import { downloadContentFromMessage, encryptedStream, generateThumbnail, getAudioDuration, getAudioWaveform, MediaDownloadOptions } from './messages-media'

type MediaUploadData = {
	media: WAMediaUpload
	caption?: string
	ptt?: boolean
	ptv?: boolean
	seconds?: number
	gifPlayback?: boolean
	fileName?: string
	jpegThumbnail?: string
	mimetype?: string
	width?: number
	height?: number
	waveform?: Uint8Array
	backgroundArgb?: number
}

const MIMETYPE_MAP: { [T in MediaType]?: string } = {
	image: 'image/jpeg',
	video: 'video/mp4',
	document: 'application/pdf',
	audio: 'audio/ogg; codecs=opus',
	sticker: 'image/webp',
	'product-catalog-image': 'image/jpeg',
}

const MessageTypeProto = {
	'image': WAProto.Message.ImageMessage,
	'video': WAProto.Message.VideoMessage,
	'audio': WAProto.Message.AudioMessage,
	'sticker': WAProto.Message.StickerMessage,
   	'document': WAProto.Message.DocumentMessage,
} as const

/**
 * Uses a regex to test whether the string contains a URL, and returns the URL if it does.
 * @param text eg. hello https://google.com
 * @returns the URL, eg. https://google.com
 */
export const extractUrlFromText = (text: string) => text.match(URL_REGEX)?.[0]

export const generateLinkPreviewIfRequired = async(text: string, getUrlInfo: MessageGenerationOptions['getUrlInfo'], logger: MessageGenerationOptions['logger']) => {
	const url = extractUrlFromText(text)
	if(!!getUrlInfo && url) {
		try {
			const urlInfo = await getUrlInfo(url)
			return urlInfo
		} catch(error) { // ignore if fails
			logger?.warn({ trace: error.stack }, 'url generation failed')
		}
	}
}

const assertColor = async(color) => {
	let assertedColor
	if(typeof color === 'number') {
		assertedColor = color > 0 ? color : 0xffffffff + Number(color) + 1
	} else {
		let hex = color.trim().replace('#', '')
		if(hex.length <= 6) {
			hex = 'FF' + hex.padStart(6, '0')
		}

		assertedColor = parseInt(hex, 16)
		return assertedColor
	}
}

export const prepareWAMessageMedia = async(
	message: AnyMediaMessageContent,
	options: MediaGenerationOptions
) => {
	const logger = options.logger

	let mediaType: typeof MEDIA_KEYS[number] | undefined
	for(const key of MEDIA_KEYS) {
		if(key in message) {
			mediaType = key
		}
	}

	if(!mediaType) {
		throw new Boom('Invalid media type', { statusCode: 400 })
	}

	const uploadData: MediaUploadData = {
		...message,
		media: message[mediaType]
	}
	delete uploadData[mediaType]
	// check if cacheable + generate cache key
	const cacheableKey = typeof uploadData.media === 'object' &&
			('url' in uploadData.media) &&
			!!uploadData.media.url &&
			!!options.mediaCache && (
	// generate the key
		mediaType + ':' + uploadData.media.url.toString()
	)

	if(mediaType === 'document' && !uploadData.fileName) {
		uploadData.fileName = 'file'
	}

	if(!uploadData.mimetype) {
		uploadData.mimetype = MIMETYPE_MAP[mediaType]
	}

	// check for cache hit
	if(cacheableKey) {
		const mediaBuff = options.mediaCache!.get<Buffer>(cacheableKey)
		if(mediaBuff) {
			logger?.debug({ cacheableKey }, 'got media cache hit')

			const obj = WAProto.Message.decode(mediaBuff)
			const key = `${mediaType}Message`

			Object.assign(obj[key], { ...uploadData, media: undefined })

			return obj
		}
	}

	const requiresDurationComputation = mediaType === 'audio' && typeof uploadData.seconds === 'undefined'
	const requiresThumbnailComputation = (mediaType === 'image' || mediaType === 'video') &&
										(typeof uploadData['jpegThumbnail'] === 'undefined')
	const requiresWaveformProcessing = mediaType === 'audio' && uploadData.ptt === true
	const requiresAudioBackground = options.backgroundColor && mediaType === 'audio' && uploadData.ptt === true
	const requiresOriginalForSomeProcessing = requiresDurationComputation || requiresThumbnailComputation
	const {
		mediaKey,
		encWriteStream,
		bodyPath,
		fileEncSha256,
		fileSha256,
		fileLength,
		didSaveToTmpPath
	} = await encryptedStream(
		uploadData.media,
		options.mediaTypeOverride || mediaType,
		{
			logger,
			saveOriginalFileIfRequired: requiresOriginalForSomeProcessing,
			opts: options.options
		}
	)
	 // url safe Base64 encode the SHA256 hash of the body
	const fileEncSha256B64 = fileEncSha256.toString('base64')
	const [{ mediaUrl, directPath }] = await Promise.all([
		(async() => {
			const result = await options.upload(
				encWriteStream,
				{ fileEncSha256B64, mediaType, timeoutMs: options.mediaUploadTimeoutMs }
			)
			logger?.debug({ mediaType, cacheableKey }, 'uploaded media')
			return result
		})(),
		(async() => {
			try {
				if(requiresThumbnailComputation) {
					const {
						thumbnail,
						originalImageDimensions
					} = await generateThumbnail(bodyPath!, mediaType as 'image' | 'video', options)
					uploadData.jpegThumbnail = thumbnail
					if(!uploadData.width && originalImageDimensions) {
						uploadData.width = originalImageDimensions.width
						uploadData.height = originalImageDimensions.height
						logger?.debug('set dimensions')
					}

					logger?.debug('generated thumbnail')
				}

				if(requiresDurationComputation) {
					uploadData.seconds = await getAudioDuration(bodyPath!)
					logger?.debug('computed audio duration')
				}

				if(requiresWaveformProcessing) {
					uploadData.waveform = await getAudioWaveform(bodyPath!, logger)
					logger?.debug('processed waveform')
				}

				if(requiresAudioBackground) {
					uploadData.backgroundArgb = await assertColor(options.backgroundColor)
					logger?.debug('computed backgroundColor audio status')
				}
			} catch(error) {
				logger?.warn({ trace: error.stack }, 'failed to obtain extra info')
			}
		})(),
	])
		.finally(
			async() => {
				encWriteStream.destroy()
				// remove tmp files
				if(didSaveToTmpPath && bodyPath) {
					await fs.unlink(bodyPath)
					logger?.debug('removed tmp files')
				}
			}
		)

	const obj = WAProto.Message.fromObject({
		[`${mediaType}Message`]: MessageTypeProto[mediaType].fromObject(
			{
				url: mediaUrl,
				directPath,
				mediaKey,
				fileEncSha256,
				fileSha256,
				fileLength,
				mediaKeyTimestamp: unixTimestampSeconds(),
				...uploadData,
				media: undefined
			}
		)
	})

	if(uploadData.ptv) {
		obj.ptvMessage = obj.videoMessage
		delete obj.videoMessage
	}

	if(cacheableKey) {
		logger?.debug({ cacheableKey }, 'set cache')
		options.mediaCache!.set(cacheableKey, WAProto.Message.encode(obj).finish())
	}

	return obj
}

export const prepareDisappearingMessageSettingContent = (ephemeralExpiration?: number) => {
	ephemeralExpiration = ephemeralExpiration || 0
	const content: WAMessageContent = {
		ephemeralMessage: {
			message: {
				protocolMessage: {
					type: WAProto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING,
					ephemeralExpiration
				}
			}
		}
	}
	return WAProto.Message.fromObject(content)
}

/**
 * Generate forwarded message content like WA does
 * @param message the message to forward
 * @param options.forceForward will show the message as forwarded even if it is from you
 */
export const generateForwardMessageContent = (
	message: WAMessage,
	forceForward?: boolean
) => {
	let content = message.message
	if(!content) {
		throw new Boom('no content in message', { statusCode: 400 })
	}

	// hacky copy
	content = normalizeMessageContent(content)
	content = proto.Message.decode(proto.Message.encode(content!).finish())

	let key = Object.keys(content)[0] as MessageType

	let score = content[key].contextInfo?.forwardingScore || 0
	score += message.key.fromMe && !forceForward ? 0 : 1
	if(key === 'conversation') {
		content.extendedTextMessage = { text: content[key] }
		delete content.conversation

		key = 'extendedTextMessage'
	}

	if(score > 0) {
		content[key].contextInfo = { forwardingScore: score, isForwarded: true }
	} else {
		content[key].contextInfo = {}
	}

	return content
}

export const generateWAMessageContent = async(
	message: AnyMessageContent,
	options: MessageContentGenerationOptions
) => {
	let m: WAMessageContent = {}
	if('text' in message) {
		const extContent = { text: message.text } as WATextMessage

		let urlInfo = message.linkPreview
		if(typeof urlInfo === 'undefined') {
			urlInfo = await generateLinkPreviewIfRequired(message.text, options.getUrlInfo, options.logger)
		}

		if(urlInfo) {
			extContent.canonicalUrl = urlInfo['canonical-url']
			extContent.matchedText = urlInfo['matched-text']
			extContent.jpegThumbnail = urlInfo.jpegThumbnail
			extContent.description = urlInfo.description
			extContent.title = urlInfo.title
			extContent.previewType = 0

			const img = urlInfo.highQualityThumbnail
			if(img) {
				extContent.thumbnailDirectPath = img.directPath
				extContent.mediaKey = img.mediaKey
				extContent.mediaKeyTimestamp = img.mediaKeyTimestamp
				extContent.thumbnailWidth = img.width
				extContent.thumbnailHeight = img.height
				extContent.thumbnailSha256 = img.fileSha256
				extContent.thumbnailEncSha256 = img.fileEncSha256
			}
		}

		if(options.backgroundColor) {
			extContent.backgroundArgb = await assertColor(options.backgroundColor)
		}

		if(options.font) {
			extContent.font = options.font
		}

		m.extendedTextMessage = extContent
	} else if('contacts' in message) {
		const contactLen = message.contacts.contacts.length
		if(!contactLen) {
			throw new Boom('require atleast 1 contact', { statusCode: 400 })
		}

		if(contactLen === 1) {
			m.contactMessage = WAProto.Message.ContactMessage.fromObject(message.contacts.contacts[0])
		} else {
			m.contactsArrayMessage = WAProto.Message.ContactsArrayMessage.fromObject(message.contacts)
		}
	} else if('location' in message) {
		m.locationMessage = WAProto.Message.LocationMessage.fromObject(message.location)
	} else if('react' in message) {
		if(!message.react.senderTimestampMs) {
			message.react.senderTimestampMs = Date.now()
		}

		m.reactionMessage = WAProto.Message.ReactionMessage.fromObject(message.react)
	} else if('delete' in message) {
		m.protocolMessage = {
			key: message.delete,
			type: WAProto.Message.ProtocolMessage.Type.REVOKE
		}
	} else if('forward' in message) {
		m = generateForwardMessageContent(
			message.forward,
			message.force
		)
	} else if('disappearingMessagesInChat' in message) {
		const exp = typeof message.disappearingMessagesInChat === 'boolean' ?
			(message.disappearingMessagesInChat ? WA_DEFAULT_EPHEMERAL : 0) :
			message.disappearingMessagesInChat
		m = prepareDisappearingMessageSettingContent(exp)
	} else if('groupInvite' in message) {
		m.groupInviteMessage = {}
		m.groupInviteMessage.inviteCode = message.groupInvite.inviteCode
		m.groupInviteMessage.inviteExpiration = message.groupInvite.inviteExpiration
		m.groupInviteMessage.caption = message.groupInvite.text

		m.groupInviteMessage.groupJid = message.groupInvite.jid
		m.groupInviteMessage.groupName = message.groupInvite.subject
		//TODO: use built-in interface and get disappearing mode info etc.
		//TODO: cache / use store!?
		if(options.getProfilePicUrl) {
			const pfpUrl = await options.getProfilePicUrl(message.groupInvite.jid, 'preview')
			if(pfpUrl) {
				const resp = await axios.get(pfpUrl, { responseType: 'arraybuffer' })
				if(resp.status === 200) {
					m.groupInviteMessage.jpegThumbnail = resp.data
				}
			}
		}
	} else if('pin' in message) {
		m.pinInChatMessage = {}
		m.messageContextInfo = {}

		m.pinInChatMessage.key = message.pin
		m.pinInChatMessage.type = message.type
		m.pinInChatMessage.senderTimestampMs = Date.now()

		m.messageContextInfo.messageAddOnDurationInSecs = message.type === 1 ? message.time || 86400 : 0
	} else if('buttonReply' in message) {
		switch (message.type) {
		case 'template':
			m.templateButtonReplyMessage = {
				selectedDisplayText: message.buttonReply.displayText,
				selectedId: message.buttonReply.id,
				selectedIndex: message.buttonReply.index,
			}
			break
		case 'plain':
			m.buttonsResponseMessage = {
				selectedButtonId: message.buttonReply.id,
				selectedDisplayText: message.buttonReply.displayText,
				type: proto.Message.ButtonsResponseMessage.Type.DISPLAY_TEXT,
			}
			break
		}
	} else if('ptv' in message && message.ptv) {
		const { videoMessage } = await prepareWAMessageMedia(
			{ video: message.video },
			options
		)
		m.ptvMessage = videoMessage
	} else if('product' in message) {
		const { imageMessage } = await prepareWAMessageMedia(
			{ image: message.product.productImage },
			options
		)
		m.productMessage = WAProto.Message.ProductMessage.fromObject({
			...message,
			product: {
				...message.product,
				productImage: imageMessage,
			}
		})
	} else if('listReply' in message) {
		m.listResponseMessage = { ...message.listReply }
	} else if('poll' in message) {
		message.poll.selectableCount ||= 0
		message.poll.toAnnouncementGroup ||= false

		if(!Array.isArray(message.poll.values)) {
			throw new Boom('Invalid poll values', { statusCode: 400 })
		}

		if(
			message.poll.selectableCount < 0
			|| message.poll.selectableCount > message.poll.values.length
		) {
			throw new Boom(
				`poll.selectableCount in poll should be >= 0 and <= ${message.poll.values.length}`,
				{ statusCode: 400 }
			)
		}

		m.messageContextInfo = {
			// encKey
			messageSecret: message.poll.messageSecret || randomBytes(32),
		}

		const pollCreationMessage = {
			name: message.poll.name,
			selectableOptionsCount: message.poll.selectableCount,
			options: message.poll.values.map(optionName => ({ optionName })),
		}

		if(message.poll.toAnnouncementGroup) {
			// poll v2 is for community announcement groups (single select and multiple)
			m.pollCreationMessageV2 = pollCreationMessage
		} else {
			if(message.poll.selectableCount > 0) {
				//poll v3 is for single select polls
				m.pollCreationMessageV3 = pollCreationMessage
			} else {
				// poll v3 for multiple choice polls
				m.pollCreationMessage = pollCreationMessage
			}
		}
	} else if('sharePhoneNumber' in message) {
		m.protocolMessage = {
			type: proto.Message.ProtocolMessage.Type.SHARE_PHONE_NUMBER
		}
	} else if('requestPhoneNumber' in message) {
		m.requestPhoneNumberMessage = {}
	} else {
		m = await prepareWAMessageMedia(
			message,
			options
		)
	}

	if('viewOnce' in message && !!message.viewOnce) {
		m = { viewOnceMessage: { message: m } }
	}

	if('mentions' in message && message.mentions?.length) {
		const [messageType] = Object.keys(m)
		m[messageType].contextInfo = m[messageType] || { }
		m[messageType].contextInfo.mentionedJid = message.mentions
	}

	if('edit' in message) {
		m = {
			protocolMessage: {
				key: message.edit,
				editedMessage: m,
				timestampMs: Date.now(),
				type: WAProto.Message.ProtocolMessage.Type.MESSAGE_EDIT
			}
		}
	}

	if('contextInfo' in message && !!message.contextInfo) {
		const [messageType] = Object.keys(m)
		m[messageType] = m[messageType] || {}
		m[messageType].contextInfo = message.contextInfo
	}

	return WAProto.Message.fromObject(m)
}

export const generateWAMessageFromContent = (
	jid: string,
	message: WAMessageContent,
	options: MessageGenerationOptionsFromContent
) => {
	// set timestamp to now
	// if not specified
	if(!options.timestamp) {
		options.timestamp = new Date()
	}

	const innerMessage = normalizeMessageContent(message)!
	const key: string = getContentType(innerMessage)!
	const timestamp = unixTimestampSeconds(options.timestamp)
	const { quoted, userJid } = options

	if(quoted) {
		const participant = quoted.key.fromMe ? userJid : (quoted.participant || quoted.key.participant || quoted.key.remoteJid)

		let quotedMsg = normalizeMessageContent(quoted.message)!
		const msgType = getContentType(quotedMsg)!
		// strip any redundant properties
		quotedMsg = proto.Message.fromObject({ [msgType]: quotedMsg[msgType] })

		const quotedContent = quotedMsg[msgType]
		if(typeof quotedContent === 'object' && quotedContent && 'contextInfo' in quotedContent) {
			delete quotedContent.contextInfo
		}

		const contextInfo: proto.IContextInfo = innerMessage[key].contextInfo || { }
		contextInfo.participant = jidNormalizedUser(participant!)
		contextInfo.stanzaId = quoted.key.id
		contextInfo.quotedMessage = quotedMsg

		// if a participant is quoted, then it must be a group
		// hence, remoteJid of group must also be entered
		if(jid !== quoted.key.remoteJid) {
			contextInfo.remoteJid = quoted.key.remoteJid
		}

		innerMessage[key].contextInfo = contextInfo
	}

	if(
		// if we want to send a disappearing message
		!!options?.ephemeralExpiration &&
		// and it's not a protocol message -- delete, toggle disappear message
		key !== 'protocolMessage' &&
		// already not converted to disappearing message
		key !== 'ephemeralMessage'
	) {
		innerMessage[key].contextInfo = {
			...(innerMessage[key].contextInfo || {}),
			expiration: options.ephemeralExpiration || WA_DEFAULT_EPHEMERAL,
			//ephemeralSettingTimestamp: options.ephemeralOptions.eph_setting_ts?.toString()
		}
	}

	message = WAProto.Message.fromObject(message)

	const messageJSON = {
		key: {
			remoteJid: jid,
			fromMe: true,
			id: options?.messageId || generateMessageID(),
		},
		message: message,
		messageTimestamp: timestamp,
		messageStubParameters: [],
		participant: isJidGroup(jid) || isJidStatusBroadcast(jid) ? userJid : undefined,
		status: WAMessageStatus.PENDING
	}
	return WAProto.WebMessageInfo.fromObject(messageJSON)
}

export const generateWAMessage = async(
	jid: string,
	content: AnyMessageContent,
	options: MessageGenerationOptions,
) => {
	// ensure msg ID is with every log
	options.logger = options?.logger?.child({ msgId: options.messageId })
	return generateWAMessageFromContent(
		jid,
		await generateWAMessageContent(
			content,
			options
		),
		options
	)
}

/** Get the key to access the true type of content */
export const getContentType = (content: WAProto.IMessage | undefined) => {
	if(content) {
		const keys = Object.keys(content)
		const key = keys.find(k => (k === 'conversation' || k.includes('Message')) && k !== 'senderKeyDistributionMessage')
		return key as keyof typeof content
	}
}

/**
 * Normalizes ephemeral, view once messages to regular message content
 * Eg. image messages in ephemeral messages, in view once messages etc.
 * @param content
 * @returns
 */
export const normalizeMessageContent = (content: WAMessageContent | null | undefined): WAMessageContent | undefined => {
	 if(!content) {
		 return undefined
	 }

	 // set max iterations to prevent an infinite loop
	 for(let i = 0;i < 5;i++) {
		 const inner = getFutureProofMessage(content)
		 if(!inner) {
			 break
		 }

		 content = inner.message
	 }

	 return content!

	 function getFutureProofMessage(message: typeof content) {
		 return (
			 message?.ephemeralMessage
			 || message?.viewOnceMessage
			 || message?.documentWithCaptionMessage
			 || message?.viewOnceMessageV2
			 || message?.viewOnceMessageV2Extension
			 || message?.editedMessage
		 )
	 }
}

/**
 * Extract the true message content from a message
 * Eg. extracts the inner message from a disappearing message/view once message
 */
export const extractMessageContent = (content: WAMessageContent | undefined | null): WAMessageContent | undefined => {
	const extractFromTemplateMessage = (msg: proto.Message.TemplateMessage.IHydratedFourRowTemplate | proto.Message.IButtonsMessage) => {
		if(msg.imageMessage) {
			return { imageMessage: msg.imageMessage }
		} else if(msg.documentMessage) {
			return { documentMessage: msg.documentMessage }
		} else if(msg.videoMessage) {
			return { videoMessage: msg.videoMessage }
		} else if(msg.locationMessage) {
			return { locationMessage: msg.locationMessage }
		} else {
			return {
				conversation:
					'contentText' in msg
						? msg.contentText
						: ('hydratedContentText' in msg ? msg.hydratedContentText : '')
			}
		}
	}

	content = normalizeMessageContent(content)

	if(content?.buttonsMessage) {
	  return extractFromTemplateMessage(content.buttonsMessage)
	}

	if(content?.templateMessage?.hydratedFourRowTemplate) {
		return extractFromTemplateMessage(content?.templateMessage?.hydratedFourRowTemplate)
	}

	if(content?.templateMessage?.hydratedTemplate) {
		return extractFromTemplateMessage(content?.templateMessage?.hydratedTemplate)
	}

	if(content?.templateMessage?.fourRowTemplate) {
		return extractFromTemplateMessage(content?.templateMessage?.fourRowTemplate)
	}

	return content
}

/**
 * Returns the device predicted by message ID
 */
export const getDevice = (id: string) => /^3A.{18}$/.test(id) ? 'ios' : /^3E.{20}$/.test(id) ? 'web' : /^(.{21}|.{32})$/.test(id) ? 'android' : /^.{18}$/.test(id) ? 'desktop' : 'unknown'

/** Upserts a receipt in the message */
export const updateMessageWithReceipt = (msg: Pick<WAMessage, 'userReceipt'>, receipt: MessageUserReceipt) => {
	msg.userReceipt = msg.userReceipt || []
	const recp = msg.userReceipt.find(m => m.userJid === receipt.userJid)
	if(recp) {
		Object.assign(recp, receipt)
	} else {
		msg.userReceipt.push(receipt)
	}
}

/** Update the message with a new reaction */
export const updateMessageWithReaction = (msg: Pick<WAMessage, 'reactions'>, reaction: proto.IReaction) => {
	const authorID = getKeyAuthor(reaction.key)

	const reactions = (msg.reactions || [])
		.filter(r => getKeyAuthor(r.key) !== authorID)
	if(reaction.text) {
		reactions.push(reaction)
	}

	msg.reactions = reactions
}

/** Update the message with a new poll update */
export const updateMessageWithPollUpdate = (
	msg: Pick<WAMessage, 'pollUpdates'>,
	update: proto.IPollUpdate
) => {
	const authorID = getKeyAuthor(update.pollUpdateMessageKey)

	const reactions = (msg.pollUpdates || [])
		.filter(r => getKeyAuthor(r.pollUpdateMessageKey) !== authorID)
	if(update.vote?.selectedOptions?.length) {
		reactions.push(update)
	}

	msg.pollUpdates = reactions
}

type VoteAggregation = {
	name: string
	voters: string[]
}

/**
 * Aggregates all poll updates in a poll.
 * @param msg the poll creation message
 * @param meId your jid
 * @returns A list of options & their voters
 */
export function getAggregateVotesInPollMessage(
	{ message, pollUpdates }: Pick<WAMessage, 'pollUpdates' | 'message'>,
	meId?: string
) {
	const opts = message?.pollCreationMessage?.options || message?.pollCreationMessageV2?.options || message?.pollCreationMessageV3?.options || []
	const voteHashMap = opts.reduce((acc, opt) => {
		const hash = sha256(Buffer.from(opt.optionName || '')).toString()
		acc[hash] = {
			name: opt.optionName || '',
			voters: []
		}
		return acc
	}, {} as { [_: string]: VoteAggregation })

	for(const update of pollUpdates || []) {
		const { vote } = update
		if(!vote) {
			continue
		}

		for(const option of vote.selectedOptions || []) {
			const hash = option.toString()
			let data = voteHashMap[hash]
			if(!data) {
				voteHashMap[hash] = {
					name: 'Unknown',
					voters: []
				}
				data = voteHashMap[hash]
			}

			voteHashMap[hash].voters.push(
				getKeyAuthor(update.pollUpdateMessageKey, meId)
			)
		}
	}

	return Object.values(voteHashMap)
}

/** Given a list of message keys, aggregates them by chat & sender. Useful for sending read receipts in bulk */
export const aggregateMessageKeysNotFromMe = (keys: proto.IMessageKey[]) => {
	const keyMap: { [id: string]: { jid: string, participant: string | undefined, messageIds: string[] } } = { }
	for(const { remoteJid, id, participant, fromMe } of keys) {
		if(!fromMe) {
			const uqKey = `${remoteJid}:${participant || ''}`
			if(!keyMap[uqKey]) {
				keyMap[uqKey] = {
					jid: remoteJid!,
					participant: participant!,
					messageIds: []
				}
			}

			keyMap[uqKey].messageIds.push(id!)
		}
	}

	return Object.values(keyMap)
}

type DownloadMediaMessageContext = {
	reuploadRequest: (msg: WAMessage) => Promise<WAMessage>
	logger: ILogger
}

const REUPLOAD_REQUIRED_STATUS = [410, 404]

/**
 * Downloads the given message. Throws an error if it's not a media message
 */
export const downloadMediaMessage = async<Type extends 'buffer' | 'stream'>(
	message: WAMessage,
	type: Type,
	options: MediaDownloadOptions,
	ctx?: DownloadMediaMessageContext
) => {
	const result = await downloadMsg()
		.catch(async(error) => {
			if(ctx && axios.isAxiosError(error) && // check if the message requires a reupload
					REUPLOAD_REQUIRED_STATUS.includes(error.response?.status!)) {
				ctx.logger.info({ key: message.key }, 'sending reupload media request...')
				// request reupload
				message = await ctx.reuploadRequest(message)
				const result = await downloadMsg()
				return result
			}

			throw error
		})

	return result as Type extends 'buffer' ? Buffer : Transform

	async function downloadMsg() {
		const mContent = extractMessageContent(message.message)
		if(!mContent) {
			throw new Boom('No message present', { statusCode: 400, data: message })
		}

		const contentType = getContentType(mContent)
		let mediaType = contentType?.replace('Message', '') as MediaType
		const media = mContent[contentType!]

		if(!media || typeof media !== 'object' || (!('url' in media) && !('thumbnailDirectPath' in media))) {
			throw new Boom(`"${contentType}" message is not a media message`)
		}

		let download: DownloadableMessage
		if('thumbnailDirectPath' in media && !('url' in media)) {
			download = {
				directPath: media.thumbnailDirectPath,
				mediaKey: media.mediaKey
			}
			mediaType = 'thumbnail-link'
		} else {
			download = media
		}

		const stream = await downloadContentFromMessage(download, mediaType, options)
		if(type === 'buffer') {
			const bufferArray: Buffer[] = []
			for await (const chunk of stream) {
				bufferArray.push(chunk)
			}

			return Buffer.concat(bufferArray)
		}

		return stream
	}
}

/** Checks whether the given message is a media message; if it is returns the inner content */
export const assertMediaContent = (content: proto.IMessage | null | undefined) => {
	content = extractMessageContent(content)
	const mediaContent = content?.documentMessage
		|| content?.imageMessage
		|| content?.videoMessage
		|| content?.audioMessage
		|| content?.stickerMessage
	if(!mediaContent) {
		throw new Boom(
			'given message is not a media message',
			{ statusCode: 400, data: content }
		)
	}

	return mediaContent
}
