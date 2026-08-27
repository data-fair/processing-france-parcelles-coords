// @types/jsonstream ships its declarations under the lowercase name `jsonstream`,
// which TypeScript cannot resolve for the `JSONStream` specifier on a case-sensitive
// filesystem. Declare the (tiny) surface we actually use instead.
declare module 'JSONStream' {
  import type { Transform } from 'node:stream'

  export function parse (pattern: any): Transform
  export function stringify (open?: string | false, sep?: string, close?: string): Transform

  const JSONStream: { parse: typeof parse, stringify: typeof stringify }
  export default JSONStream
}
