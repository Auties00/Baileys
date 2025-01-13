import P from 'pino'

export interface ILogger {
    level: string
    child(obj: Record<string, unknown>): ILogger
    trace(obj: unknown, msg?: string)
    debug(obj: unknown, msg?: string)
    info(obj: unknown, msg?: string)
    warn(obj: unknown, msg?: string)
    error(obj: unknown, msg?: string)
}

export default P({ timestamp: () => `,"time":"${new Date().toJSON()}"` })
