import { Boom } from '@hapi/boom'
import { proto } from '../../WAProto'
import { NOISE_MODE, WA_CERT_DETAILS } from '../Defaults'
import { KeyPair } from '../Types'
import { BinaryNode, decodeBinaryNode } from '../WABinary'
import { aesDecryptGCM, aesEncryptGCM, Curve, hkdf, sha256 } from './crypto'
import { ILogger } from './logger'

const generateIV = (counter: number) => {
	const iv = new ArrayBuffer(12)
	new DataView(iv).setUint32(8, counter)

	return new Uint8Array(iv)
}

export const makeNoiseHandler = ({
	keyPair: { private: privateKey, public: publicKey },
	NOISE_HEADER,
	logger,
	routingInfo
}: {
	keyPair: KeyPair
	NOISE_HEADER: Uint8Array
	logger: ILogger
	routingInfo?: Buffer | undefined
}) => {
	logger = logger.child({ class: 'ns' })

	const authenticate = (data: Uint8Array) => {
		if(!isFinished) {
			hash = sha256(Buffer.concat([hash, data]))
		}
	}

	const encrypt = (plaintext: Uint8Array) => {
		const result = aesEncryptGCM(plaintext, encKey, generateIV(writeCounter), hash)

		writeCounter += 1

		authenticate(result)
		return result
	}

	const decrypt = (ciphertext: Uint8Array) => {
		// before the handshake is finished, we use the same counter
		// after handshake, the counters are different
		const iv = generateIV(isFinished ? readCounter : writeCounter)
		const result = aesDecryptGCM(ciphertext, decKey, iv, hash)

		if(isFinished) {
			readCounter += 1
		} else {
			writeCounter += 1
		}

		authenticate(ciphertext)
		return result
	}

	const localHKDF = (data: Uint8Array) => {
		const key = hkdf(Buffer.from(data), 64, { salt, info: '' })
		return [key.slice(0, 32), key.slice(32)]
	}

	const mixIntoKey = (data: Uint8Array) => {
		const [write, read] = localHKDF(data)
		salt = write
		encKey = read
		decKey = read
		readCounter = 0
		writeCounter = 0
	}

	const finishInit = () => {
		const [write, read] = localHKDF(new Uint8Array(0))
		encKey = write
		decKey = read
		hash = Buffer.from([])
		readCounter = 0
		writeCounter = 0
		isFinished = true
	}

	const data = Buffer.from(NOISE_MODE)
	let hash = Buffer.from(data.byteLength === 32 ? data : sha256(data))
	let salt = hash
	let encKey = hash
	let decKey = hash
	let readCounter = 0
	let writeCounter = 0
	let isFinished = false
	let sentIntro = false

	let inBytes = Buffer.alloc(0)

	authenticate(NOISE_HEADER)
	authenticate(publicKey)

	return {
		encrypt,
		decrypt,
		authenticate,
		mixIntoKey,
		finishInit,
		processHandshake: ({ serverHello }: proto.HandshakeMessage, noiseKey: KeyPair) => {
			authenticate(serverHello!.ephemeral!)
			mixIntoKey(Curve.sharedKey(privateKey, serverHello!.ephemeral!))

			const decStaticContent = decrypt(serverHello!.static!)
			mixIntoKey(Curve.sharedKey(privateKey, decStaticContent))

			const certDecoded = decrypt(serverHello!.payload!)

			const { intermediate: certIntermediate } = proto.CertChain.decode(certDecoded)

			const { issuerSerial } = proto.CertChain.NoiseCertificate.Details.decode(certIntermediate!.details!)

			if(issuerSerial !== WA_CERT_DETAILS.SERIAL) {
				throw new Boom('certification match failed', { statusCode: 400 })
			}

			const keyEnc = encrypt(noiseKey.public)
			mixIntoKey(Curve.sharedKey(noiseKey.private, serverHello!.ephemeral!))

			return keyEnc
		},
		encodeFrame: (data: Buffer | Uint8Array) => {
			if(isFinished) {
				data = encrypt(data)
			}

			let header: Buffer

			if(routingInfo) {
				header = Buffer.alloc(7)
				header.write('ED', 0, 'utf8')
				header.writeUint8(0, 2)
				header.writeUint8(1, 3)
				header.writeUint8(routingInfo.byteLength >> 16, 4)
				header.writeUint16BE(routingInfo.byteLength & 65535, 5)
				header = Buffer.concat([header, routingInfo, NOISE_HEADER])
			} else {
				header = Buffer.from(NOISE_HEADER)
			}

			const introSize = sentIntro ? 0 : header.length
			const frame = Buffer.alloc(introSize + 3 + data.byteLength)

			if(!sentIntro) {
				frame.set(header)
				sentIntro = true
			}

			frame.writeUInt8(data.byteLength >> 16, introSize)
			frame.writeUInt16BE(65535 & data.byteLength, introSize + 1)
			frame.set(data, introSize + 3)

			return frame
		},
		decodeFrame: async(newData: Buffer | Uint8Array, onFrame: (buff: Uint8Array | BinaryNode) => void) => {
			// the binary protocol uses its own framing mechanism
			// on top of the WS frames
			// so we get this data and separate out the frames
			const getBytesSize = () => {
				if(inBytes.length >= 3) {
					return (inBytes.readUInt8() << 16) | inBytes.readUInt16BE(1)
				}
			}

			inBytes = Buffer.concat([ inBytes, newData ])

			logger.trace(`recv ${newData.length} bytes, total recv ${inBytes.length} bytes`)

			let size = getBytesSize()
			while(size && inBytes.length >= size + 3) {
				let frame: Uint8Array | BinaryNode = inBytes.slice(3, size + 3)
				inBytes = inBytes.slice(size + 3)

				if(isFinished) {
					const result = decrypt(frame)
					frame = await decodeBinaryNode(result)
				}

				logger.trace({ msg: (frame as BinaryNode)?.attrs?.id }, 'recv frame')

				onFrame(frame)
				size = getBytesSize()
			}
		}
	}
}