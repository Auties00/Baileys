
import { AxiosRequestConfig } from 'axios'
import type { Agent } from 'https'
import type { URL } from 'url'
import { proto } from '../../WAProto'
import { ILogger } from '../Utils/logger'
import { AuthenticationState, SignalAuthState, TransactionCapabilityOptions } from './Auth'
import { GroupMetadata } from './GroupMetadata'
import { MediaConnInfo } from './Message'
import { SignalRepository } from './Signal'

export type WAVersion = [number, number, number]
export type WABrowserDescription = [string, string, string]

export type CacheStore = {
    /** get a cached key and change the stats */
    get<T>(key: string): T | undefined
    /** set a key in the cache */
    set<T>(key: string, value: T): void
    /** delete a key from the cache */
    del(key: string): void
    /** flush all data */
    flushAll(): void
}

export type SocketConfig = {
    /** the WS url to connect to WA */
    waWebSocketUrl: string | URL
    /** Fails the connection if the socket times out in this interval */
    connectTimeoutMs: number
    /** Default timeout for queries, undefined for no timeout */
    defaultQueryTimeoutMs: number | undefined
    /** ping-pong interval for WS connection */
    keepAliveIntervalMs: number
	/** should baileys use the mobile api instead of the multi device api
     * @deprecated This feature has been removed
    */
	mobile?: boolean
    /** proxy agent */
    agent?: Agent
    /** logger */
    logger: ILogger
    /** version to connect with */
    version: WAVersion
    /** override browser config */
    browser: WABrowserDescription
    /** agent used for fetch requests -- uploading/downloading media */
    fetchAgent?: Agent
    /** should the QR be printed in the terminal */
    printQRInTerminal: boolean
    /** should events be emitted for actions done by this socket connection */
    emitOwnEvents: boolean
    /** custom upload hosts to upload media to */
    customUploadHosts: MediaConnInfo['hosts']
    /** time to wait between sending new retry requests */
    retryRequestDelayMs: number
    /** max retry count */
    maxMsgRetryCount: number
    /** time to wait for the generation of the next QR in ms */
    qrTimeout?: number
    /** provide an auth state object to maintain the auth state */
    auth: AuthenticationState
    /** manage history processing with this control; by default will sync up everything */
    shouldSyncHistoryMessage: (msg: proto.Message.IHistorySyncNotification) => boolean
    /** transaction capability options for SignalKeyStore */
    transactionOpts: TransactionCapabilityOptions
    /** marks the client as online whenever the socket successfully connects */
    markOnlineOnConnect: boolean

    /** provide a cache to store media, so does not have to be re-uploaded */
    mediaCache?: CacheStore
    /**
     * map to store the retry counts for failed messages;
     * used to determine whether to retry a message or not */
    msgRetryCounterCache?: CacheStore
    /** provide a cache to store a user's device list */
    userDevicesCache?: CacheStore
    /** cache to store call offers */
    callOfferCache?: CacheStore
    /** cache to track placeholder resends */
    placeholderResendCache?: CacheStore
    /** width for link preview images */
    linkPreviewImageThumbnailWidth: number
    /** Should Baileys ask the phone for full history, will be received async */
    syncFullHistory: boolean
    /** Should baileys fire init queries automatically, default true */
    fireInitQueries: boolean
    /**
     * generate a high quality link preview,
     * entails uploading the jpegThumbnail to WA
     * */
    generateHighQualityLinkPreview: boolean

    /**
     * Returns if a jid should be ignored,
     * no event for that jid will be triggered.
     * Messages from that jid will also not be decrypted
     * */
    shouldIgnoreJid: (jid: string) => boolean | undefined

    /**
     * Optionally patch the message before sending out
     * */
    patchMessageBeforeSending: (
        msg: proto.IMessage,
        recipientJids: string[],
    ) => Promise<proto.IMessage> | proto.IMessage

    /** verify app state MACs */
    appStateMacVerification: {
        patch: boolean
        snapshot: boolean
    }

    /** options for axios */
    options: AxiosRequestConfig<{}>
    /**
     * fetch a message from your store
     * implement this so that messages failed to send
     * (solves the "this message can take a while" issue) can be retried
     * */
    getMessage: (key: proto.IMessageKey) => Promise<proto.IMessage | undefined>

    /** cached group metadata, use to prevent redundant requests to WA & speed up msg sending */
    cachedGroupMetadata: (jid: string) => Promise<GroupMetadata | undefined>

    makeSignalRepository: (auth: SignalAuthState) => SignalRepository
}
