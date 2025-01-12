import type { proto } from '../../WAProto'
import type { AccountSettings } from './Auth'
import type { BufferedEventData } from './Events'
import type { LabelActionBody } from './Label'
import type { ChatLabelAssociationActionBody } from './LabelAssociation'
import type { MessageLabelAssociationActionBody } from './LabelAssociation'
import type { MinimalMessage, WAMessageKey } from './Message'

/** privacy settings in WhatsApp Web */
export type WAPrivacyValue = 'all' | 'contacts' | 'contact_blacklist' | 'none'

export type WAPrivacyOnlineValue = 'all' | 'match_last_seen'

export type WAPrivacyGroupAddValue = 'all' | 'contacts' | 'contact_blacklist'

export type WAReadReceiptsValue = 'all' | 'none'

export type WAPrivacyCallValue = 'all' | 'known'

export type WAPrivacyMessagesValue = 'all' | 'contacts'

/** set of statuses visible to other people; see updatePresence() in WhatsAppWeb.Send */
export type WAPresence = 'unavailable' | 'available' | 'composing' | 'recording' | 'paused'

export const ALL_WA_PATCH_NAMES = ['critical_block', 'critical_unblock_low', 'regular_high', 'regular_low', 'regular'] as const

export type WAPatchName = typeof ALL_WA_PATCH_NAMES[number]

export interface PresenceData {
    lastKnownPresence: WAPresence
    lastSeen?: number
}

export type ChatMutation = {
    syncAction: proto.ISyncActionData
    index: string[]
}

export type WAPatchCreate = {
    syncAction: proto.ISyncActionValue
    index: string[]
    type: WAPatchName
    apiVersion: number
    operation: proto.SyncdMutation.SyncdOperation
}

export type Chat = proto.IConversation & {
    /** unix timestamp of when the last message was received in the chat */
    lastMessageRecvTimestamp?: number
}

export type ChatUpdate = Partial<Chat & {
    /**
     * if specified in the update,
     * the EV buffer will check if the condition gets fulfilled before applying the update
     * Right now, used to determine when to release an app state sync event
     *
     * @returns true, if the update should be applied;
     * false if it can be discarded;
     * undefined if the condition is not yet fulfilled
     * */
    conditional: (bufferedData: BufferedEventData) => boolean | undefined
}>

/**
 * the last messages in a chat, sorted reverse-chronologically. That is, the latest message should be first in the chat
 * for MD modifications, the last message in the array (i.e. the earlist message) must be the last message recv in the chat
 * */
export type LastMessageList = MinimalMessage[] | proto.SyncActionValue.ISyncActionMessageRange

export type ChatModification =
    {
        archive: boolean
        lastMessages: LastMessageList
    }
    | { pushNameSetting: string }
    | { pin: boolean }
    | {
        /** mute for duration, or provide timestamp of mute to remove*/
        mute: number | null
    }
    | {
        clear: boolean
    } | {
        deleteForMe: { deleteMedia: boolean, key: WAMessageKey, timestamp: number }
    }
    | {
        star: {
            messages: { id: string, fromMe?: boolean }[]
            star: boolean
        }
    }
    | {
        markRead: boolean
        lastMessages: LastMessageList
    }
    | { delete: true, lastMessages: LastMessageList }
    // Label
    | { addLabel: LabelActionBody }
    // Label assosiation
    | { addChatLabel: ChatLabelAssociationActionBody }
    | { removeChatLabel: ChatLabelAssociationActionBody }
    | { addMessageLabel: MessageLabelAssociationActionBody }
    | { removeMessageLabel: MessageLabelAssociationActionBody }

export type InitialReceivedChatsState = {
    [jid: string]: {
        /** the last message received from the other party */
        lastMsgRecvTimestamp?: number
        /** the absolute last message in the chat */
        lastMsgTimestamp: number
    }
}

export type InitialAppStateSyncOptions = {
    accountSettings: AccountSettings
}
