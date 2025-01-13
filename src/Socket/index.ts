import { DEFAULT_CONNECTION_CONFIG } from '../Defaults'
import { UserFacingSocketConfig } from '../Types'
import { makeCommunitiesSocket } from './communities'

// export the last socket layer
const makeWASocket = (config: UserFacingSocketConfig) => (
	makeCommunitiesSocket({
		...DEFAULT_CONNECTION_CONFIG,
		...config
	})
)

export default makeWASocket