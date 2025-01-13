/**
 * @file SQLite Store Management for Baileys
 * @license MIT
 *
 * Use and modify this code freely under the MIT license. If you use this in your projects, attribution would be appreciated.
 *
 * Author: Zaid (GitHub: hacxk)
 *
 * This module provides a SQLite-based storage solution for managing Baileys data, including message history,
 * contacts, groups, and connection state. It uses SQLite to persist data and LRU cache for efficient data
 * retrieval. It supports CRUD operations for managing messages, contacts, group metadata, and more.
 */

import {
  BaileysEventEmitter,
  ConnectionState,
  WAMessage,
  Contact,
  GroupMetadata,
  Chat,
  WAMessageKey,
  ChatUpdate,
  PresenceData,
} from "../Types";

import {
  delay
} from "../Utils"

import { proto } from "../../WAProto";
import makeWASocket from "../Socket"

import sqlite3 from "sqlite3";
import { open } from "sqlite";
import LRUCache from "lru-cache";
import pino from "pino";
import { Label } from "../Types/Label";
import { LabelAssociation } from "../Types/LabelAssociation";
import path from 'path';
import { existsSync } from "fs";
import { mkdir } from 'fs/promises';

type WASocket = ReturnType<typeof makeWASocket>

export interface makeInSQLiteStoreFunc {
  state: ConnectionState | null;
  bind: (ev: BaileysEventEmitter, sock: WASocket) => Promise<void>;
  getData: (key: string) => Promise<any>;
  setData: (key: string, data: any) => Promise<void>;
  loadMessages: (
    jid: string,
    count: number
  ) => Promise<proto.IWebMessageInfo[]>;
  loadMessage: (
    jid: string,
    id: string
  ) => Promise<proto.IWebMessageInfo | undefined>;
  loadAllGroupMetadata: () => Promise<GroupMetadata[]>;
  loadGroupMetadataByJid: (jid: string) => Promise<GroupMetadata | undefined>;
  customQuery: (query: string, params?: any[]) => Promise<any>;
  getAllContacts: () => Promise<Contact[]>;
  getGroupByJid: (jid: string) => Promise<GroupMetadata | undefined>;
  updateMessageStatus: (
    jid: string,
    id: string,
    status: "sent" | "delivered" | "read"
  ) => Promise<void>;
  removeAllData: () => Promise<void>;
  getMessageLabels: (messageId: string) => Promise<string[]>;
  mostRecentMessage: (jid: string) => Promise<proto.IWebMessageInfo>;
  fetchImageUrl: (
    jid: string,
    sock: WASocket | undefined
  ) => Promise<string | null | undefined>;
  fetchGroupMetadata: (
    jid: string,
    sock: WASocket | undefined
  ) => Promise<GroupMetadata>;
  fetchMessageReceipts: ({
    remoteJid,
    id,
  }: WAMessageKey) => Promise<proto.IUserReceipt[] | null | undefined>;
  toJSON: () => any;
  fromJSON: (json: any) => void;
}

export async function makeInSQLiteStore(
  instance_id: string,
  dbPath: string,
  logger?: pino.Logger
): Promise<makeInSQLiteStoreFunc> {
  const state: ConnectionState | null = null;
  const cache = new LRUCache<string, any>({ max: 10000 });
  const writeQueue: Array<() => Promise<void>> = [];
  let isWriting = false;
  const dbDirectory = path.dirname(dbPath);

  if (!existsSync(dbDirectory)) {
    await mkdir(dbDirectory, { recursive: true });
    logger?.info(`Created database directory: ${dbDirectory}`);
  }

  // Initialize SQLite database
  const db = await open({
    filename: dbPath,
    driver: sqlite3.Database,
  });

  if (!db) throw new Error("No SQLite database connection established");

  const log = logger || pino({ level: "info" });

  const createTablesIfNotExist = async () => {
    const queries = [
      `CREATE TABLE IF NOT EXISTS baileys_store (
                  instance_id TEXT NOT NULL,
                  key TEXT NOT NULL,
                  value TEXT,
                  PRIMARY KEY (instance_id, key)
              );`,
      `CREATE TABLE IF NOT EXISTS message_status (
                  instance_id TEXT NOT NULL,
                  jid TEXT NOT NULL,
                  message_id TEXT NOT NULL,
                  from_me INTEGER DEFAULT 0,
                  is_read INTEGER DEFAULT 0,
                  PRIMARY KEY (instance_id, jid, message_id)
              );`,
    ];

    for (const query of queries) {
      try {
        await db.exec(query);
        log.info(`Table created or already exists: ${query.split(" ")[5]}`);
      } catch (error) {
        log.error({ error, query }, "Failed to create table");
      }
    }
  };

  await createTablesIfNotExist();

  const processWriteQueue = async () => {
    if (isWriting || writeQueue.length === 0) return;
    isWriting = true;
    while (writeQueue.length > 0) {
      const writeOp = writeQueue.shift();
      if (writeOp) {
        try {
          await writeOp();
        } catch (error) {
          log.error({ error }, "Error processing write queue");
        }
      }
    }
    isWriting = false;
  };

  const saveToSQLite = async (key: string, data: any) => {
    const writeOp = async () => {
      try {
        await db.run(
          "INSERT OR REPLACE INTO baileys_store (instance_id, key, value) VALUES (?, ?, ?)",
          [instance_id, key, JSON.stringify(data)]
        );
      } catch (error) {
        log.error({ error, key }, "Failed to save data to SQLite");
      }
    };
    writeQueue.push(writeOp);
    if (!isWriting) {
      processWriteQueue();
    }
  };

  const getData = async (key: string): Promise<any> => {
    if (cache.has(key)) return cache.get(key);

    try {
      const row = await db.get(
        "SELECT value FROM baileys_store WHERE instance_id = ? AND key = ?",
        [instance_id, key]
      );

      if (row) {
        try {
          const data = JSON.parse(row.value);
          cache.set(key, data);
          return data;
        } catch (parseError) {
          log.error(
            { error: parseError, key },
            "Failed to parse data from SQLite"
          );
          return null;
        }
      }
    } catch (error) {
      log.error({ error, key }, "Failed to get data from SQLite");
    }

    return null;
  };

  const setData = async (key: string, data: any): Promise<void> => {
    cache.set(key, data);
    await saveToSQLite(key, data);
  };

  const bind = async (ev: BaileysEventEmitter, sock: WASocket) => {
    ev.on("connection.update", async (update) => {
      Object.assign(state || {}, update);
      await setData(`connection.update`, update);

      if (update.connection === "open") {
        await delay(2000);
        const groups = await sock.groupFetchAllParticipating();

        for (const groupId in groups) {
          const groupMetadata = groups[groupId];
          try {
            await setData(`group-${groupId}`, groupMetadata);
          } catch (error) {
            log.error({ error }, "Failed to save group participant");
          }
        }
      }
    });

    ev.on(
      "messaging-history.set",
      async (data: {
        chats: Chat[];
        contacts: Contact[];
        messages: WAMessage[];
        isLatest?: boolean;
        progress?: number | null;
        syncType?: proto.HistorySync.HistorySyncType;
        peerDataRequestSessionId?: string | null;
      }) => {
        const { chats, contacts, messages, isLatest } = data;
        await setData(`chats`, chats);
        await setData(`contacts`, contacts);
        for (const message of messages) {
          if (message.key && message.key.remoteJid && message.key.id) {
            await setData(
              `messages-${message.key.remoteJid}-${message.key.id}`,
              message
            );
          }
        }
        if (isLatest !== undefined) {
          await setData(`messaging-history-is-latest`, isLatest);
        }
      }
    );

    ev.on("chats.upsert", async (chats: Chat[]) => {
      await setData(`chats`, chats);
    });

    ev.on("chats.update", async (updates: ChatUpdate[]) => {
      for (const update of updates) {
        const currentUnixTime = Math.floor(Date.now() / 1000);
        if (update.id) {
          const chat =
            (await getData(`chat-${update.id}-${currentUnixTime}`)) || {};
          Object.assign(chat, update);
          await setData(`chat-${update.id}-${currentUnixTime}`, chat);
        }
      }
    });

    ev.on(
      "presence.update",
      async (update: {
        id: string;
        presences: { [participant: string]: PresenceData };
      }) => {
        await setData(`presence-${update.id}`, update.presences);
      }
    );

    ev.on("contacts.upsert", async (contacts: Contact[]) => {
      const existingContacts = (await getData(`contacts`)) || [];
      const updatedContacts = [...existingContacts, ...contacts];
      await setData(`contacts`, updatedContacts);
    });

    ev.on(
      "messages.upsert",
      async (update: { messages: WAMessage[]; type: string }) => {
        for (const message of update.messages) {
          if (message.key && message.key.remoteJid && message.key.id) {
            await setData(
              `messages-${message.key.remoteJid}-${message.key.id}`,
              message
            );
            await updateMessageStatus(
              message.key.remoteJid,
              message.key.id,
              message.key.fromMe ? "sent" : "delivered"
            );
          }
        }
      }
    );

    ev.on(
      "message-receipt.update",
      async (updates: { key: proto.IMessageKey }[]) => {
        for (const update of updates) {
          if (update.key.remoteJid && update.key.id) {
            await updateMessageStatus(
              update.key.remoteJid,
              update.key.id,
              "read"
            );
          }
        }
      }
    );

    ev.on("groups.update", async (updates: Partial<GroupMetadata>[]) => {
      for (const update of updates) {
        if (update.id) {
          const group = (await getData(`group-${update.id}`)) || {};
          Object.assign(group, update);
          await setData(`group-${update.id}`, group);
          log.info({ groupId: update.id }, "Group updated");
        }
      }
    });

    ev.on("groups.upsert", async (groupMetadata: GroupMetadata[]) => {
      for (const metadata of groupMetadata) {
        await setData(`group-${metadata.id}`, metadata);
        log.info({ groupId: metadata.id }, "Group upserted");
      }
    });

    ev.on("contacts.update", async (updates: Partial<Contact>[]) => {
      for (const update of updates) {
        if (update.id) {
          const contact = (await getData(`contact-${update.id}`)) || {};
          Object.assign(contact, update);
          await setData(`contact-${update.id}`, contact);
          log.info({ contactId: update.id }, "Contact updated");
        }
      }
    });

    ev.on("chats.delete", async (deletions: string[]) => {
      for (const chatId of deletions) {
        await setData(`chat-${chatId}`, null);
        log.info({ chatId }, "Chat deleted");
      }
    });

    ev.on(
      "messages.delete",
      async (item: { keys: WAMessageKey[] } | { jid: string; all: true }) => {
        if ("all" in item) {
          const messages = (await getData(`messages-${item.jid}`)) || {};
          for (const id in messages) {
            await setData(`messages-${item.jid}-${id}`, null);
          }
          log.info({ jid: item.jid }, "All messages deleted");
        } else {
          for (const key of item.keys) {
            if (key.remoteJid && key.id) {
              await setData(`messages-${key.remoteJid}-${key.id}`, null);
              log.info(
                { messageId: key.id, jid: key.remoteJid },
                "Message deleted"
              );
            }
          }
        }
      }
    );

    ev.on("labels.edit", async (label: Label) => {
      await setData(`label-${label.id}`, label);
    });

    ev.on(
      "labels.association",
      async (data: {
        association: LabelAssociation;
        type: "add" | "remove";
      }) => {
        const { association, type } = data;
        const key = `label-association-${association.chatId}-${association.labelId}`;
        if (type === "add") {
          await setData(key, association);
        } else if (type === "remove") {
          await setData(key, null);
        }
      }
    );
  };

  const getMessageLabels = async (messageId: string): Promise<string[]> => {
    const rows = await db.all(
      "SELECT value FROM baileys_store WHERE instance_id = ? AND key LIKE ?",
      [instance_id, `label-association-%-${messageId}`]
    );
    return rows
      .map((row) => {
        try {
          const association = JSON.parse(row.value);
          return association.labelId;
        } catch (parseError) {
          log.error({ error: parseError }, "Failed to parse label association");
          return null;
        }
      })
      .filter(Boolean);
  };

  const mostRecentMessage = async (
    jid: string
  ): Promise<proto.IWebMessageInfo> => {
    const row = await db.get(
      "SELECT value FROM baileys_store WHERE instance_id = ? AND key LIKE ? ORDER BY key DESC LIMIT 1",
      [instance_id, `messages-${jid}-%`]
    );
    if (row) {
      try {
        return JSON.parse(row.value);
      } catch (parseError) {
        log.error({ error: parseError }, "Failed to parse most recent message");
        throw parseError;
      }
    }
    throw new Error("No messages found for the given JID");
  };

  const loadMessages = async (
    jid: string,
    count: number
  ): Promise<proto.IWebMessageInfo[]> => {
    const messages: proto.IWebMessageInfo[] = [];
    const rows = await db.all(
      "SELECT value FROM baileys_store WHERE instance_id = ? AND key LIKE ? ORDER BY key DESC LIMIT ?",
      [instance_id, `messages-${jid}-%`, count]
    );
    for (const row of rows) {
      try {
        const message = JSON.parse(row.value);
        messages.push(message);
      } catch (parseError) {
        log.error({ error: parseError }, "Failed to parse message data");
      }
    }
    return messages;
  };

  const updateMessageStatus = async (
    jid: string,
    id: string,
    status: "sent" | "delivered" | "read"
  ): Promise<void> => {
    try {
      const fromMe = status === "sent" ? 1 : 0;
      const isRead = status === "read" ? 1 : 0;
      await db.run(
        "INSERT OR REPLACE INTO message_status (instance_id, jid, message_id, from_me, is_read) VALUES (?, ?, ?, ?, ?)",
        [instance_id, jid, id, fromMe, isRead]
      );
    } catch (error) {
      log.error({ error, jid, id, status }, "Failed to update message status");
    }
  };

  const removeAllData = async (): Promise<void> => {
    try {
      await db.run("DELETE FROM baileys_store WHERE instance_id = ?", [
        instance_id,
      ]);
      await db.run("DELETE FROM message_status WHERE instance_id = ?", [
        instance_id,
      ]);
      cache.clear();
      log.info({ instance_id }, "All data removed for instance");
    } catch (error) {
      log.error({ error, instance_id }, "Failed to remove all data");
      throw error;
    }
  };

  const loadMessage = async (
    jid: string,
    id: string
  ): Promise<proto.IWebMessageInfo | undefined> => {
    return await getData(`messages-${jid}-${id}`);
  };

  const loadAllGroupMetadata = async (): Promise<GroupMetadata[]> => {
    try {
      const rows = await db.all(
        "SELECT value FROM baileys_store WHERE instance_id = ? AND key LIKE ?",
        [instance_id, "group-%"]
      );
      return rows
        .map((row) => {
          try {
            console.log(row.value)
            return JSON.parse(row.value);
          } catch (parseError) {
            log.error({ error: parseError }, "Failed to parse group metadata");
            return null;
          }
        })
        .filter(Boolean);
    } catch (error) {
      log.error({ error }, "Failed to load all group metadata");
      return [];
    }
  };

  const loadGroupMetadataByJid = async (
    jid: string
  ): Promise<GroupMetadata | undefined> => {
    return await getData(`group-${jid}`);
  };

  const customQuery = async (query: string, params?: any[]): Promise<any> => {
    try {
      return await db.all(query, params);
    } catch (error) {
      log.error({ error, query }, "Failed to execute custom query");
      throw error;
    }
  };

  const getAllContacts = async (): Promise<Contact[]> => {
    try {
      const rows = await db.all(
        "SELECT value FROM baileys_store WHERE instance_id = ? AND key LIKE ?",
        [instance_id, "contact-%"]
      );
      return rows
        .map((row) => {
          try {
            return JSON.parse(row.value);
          } catch (parseError) {
            log.error({ error: parseError }, "Failed to parse contact data");
            return null;
          }
        })
        .filter(Boolean);
    } catch (error) {
      log.error({ error }, "Failed to get all contacts");
      return [];
    }
  };

  const getGroupByJid = async (
    jid: string
  ): Promise<GroupMetadata | undefined> => {
    return await getData(`group-${jid}`);
  };

  const fetchImageUrl = async (
    jid: string,
    sock: WASocket | undefined
  ): Promise<string | null | undefined> => {
    if (!sock) return undefined;
    try {
      const profilePictureUrl = await sock.profilePictureUrl(jid);
      return profilePictureUrl;
    } catch (error) {
      log.error({ error, jid }, "Failed to fetch image URL");
      return null;
    }
  };

  const fetchGroupMetadata = async (
    jid: string,
    sock: WASocket | undefined
  ): Promise<GroupMetadata> => {
    if (!sock) throw new Error("WASocket is undefined");
    try {
      const metadata = await sock.groupMetadata(jid);
      await setData(`group-${jid}`, metadata);
      return metadata;
    } catch (error) {
      log.error({ error, jid }, "Failed to fetch group metadata");
      throw error;
    }
  };

  const fetchMessageReceipts = async ({
    remoteJid,
    id,
  }: WAMessageKey): Promise<proto.IUserReceipt[] | null | undefined> => {
    const message = await loadMessage(remoteJid!, id!);
    return message?.userReceipt;
  };

  const toJSON = () => {
    return {
      chats: getData("chats"),
      contacts: getData("contacts"),
      messages: getData("messages"),
      labels: getData("labels"),
      labelAssociations: getData("labelAssociations"),
    };
  };

  const fromJSON = (json: any) => {
    const { chats, contacts, messages, labels, labelAssociations } = json;
    setData("chats", chats);
    setData("contacts", contacts);
    setData("messages", messages);
    setData("labels", labels);
    setData("labelAssociations", labelAssociations);
  };

  return {
    state,
    bind,
    getData,
    setData,
    loadMessages,
    loadMessage,
    loadAllGroupMetadata,
    loadGroupMetadataByJid,
    customQuery,
    getAllContacts,
    getGroupByJid,
    updateMessageStatus,
    removeAllData,
    getMessageLabels,
    mostRecentMessage,
    fetchImageUrl,
    fetchGroupMetadata,
    fetchMessageReceipts,
    toJSON,
    fromJSON,
  };
}
