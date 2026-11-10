// Loggable view of a MongoDB connection string. Only scheme, hosts and
// database are ever echoed, and only when they look like hosts and a database
// name; anything else is replaced by a placeholder, so a password with
// unencoded delimiters cannot land in the log by being mistaken for a host.
// The query string is dropped because its options can carry secrets too.

const SCHEME = /^mongodb(\+srv)?$/i;
const HOST = /^(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+)(:\d{1,5})?$/;
const DATABASE = /^\/[A-Za-z0-9._-]*$/;

interface ParsedMongoUri {
  description: string;
  wellFormed: boolean;
  // Pieces of the password and every query option value, in each spelling the
  // driver might echo, for scrubbing error messages.
  secretFragments: string[];
}

function spellings(piece: string): string[] {
  const out = [piece, encodeURIComponent(piece)];
  try {
    out.push(decodeURIComponent(piece));
  } catch {
    // not percent-encoded; the raw piece is already in the list
  }
  return out;
}

function fragments(password: string, query: string): string[] {
  const out = new Set<string>();
  for (const piece of password.split(':')) {
    if (piece) spellings(piece).forEach(v => out.add(v));
  }
  for (const pair of query.split('&')) {
    const value = pair.slice(pair.indexOf('=') + 1);
    if (value) spellings(value).forEach(v => out.add(v));
  }
  return [...out].sort((a, b) => b.length - a.length);
}

export function parseMongoUri(uri: string): ParsedMongoUri {
  const schemeEnd = uri.indexOf('://');
  const scheme = schemeEnd === -1 ? '' : uri.slice(0, schemeEnd);
  if (!SCHEME.test(scheme)) {
    return { description: '<connection string without a MongoDB scheme>', wellFormed: false, secretFragments: [] };
  }

  const rest = uri.slice(schemeEnd + 3);
  const authorityEnd = rest.search(/[/?#]/);
  const authority = authorityEnd === -1 ? rest : rest.slice(0, authorityEnd);
  const tail = authorityEnd === -1 ? '' : rest.slice(authorityEnd);
  const at = authority.lastIndexOf('@');
  const userinfo = at === -1 ? '' : authority.slice(0, at);
  const hosts = authority.slice(at + 1);
  const pathEnd = tail.search(/[?#]/);
  const path = pathEnd === -1 ? tail : tail.slice(0, pathEnd);
  const query = tail.startsWith('?', pathEnd) ? tail.slice(pathEnd + 1).split('#')[0] : '';

  // A second '@', or one after the authority, means a credential with
  // unencoded delimiters was cut in half: this parser and the driver's may
  // then disagree on where it ends, so nothing from it is trusted.
  const wellFormed =
    hosts.length > 0 &&
    hosts.split(',').every(host => HOST.test(host)) &&
    !userinfo.includes('@') &&
    !tail.includes('@');

  if (!wellFormed) {
    return {
      description: `${scheme}://<redacted: connection string not in the expected form>`,
      wellFormed: false,
      secretFragments: [],
    };
  }

  const colon = userinfo.indexOf(':');
  return {
    description: `${scheme}://${at === -1 ? '' : '***:***@'}${hosts}${DATABASE.test(path) ? path : ''}`,
    wellFormed: true,
    secretFragments: fragments(colon === -1 ? '' : userinfo.slice(colon + 1), query),
  };
}

export function describeMongoUri(uri: string): string {
  return parseMongoUri(uri).description;
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Driver and parser errors quote parts of the connection string in their
// messages. For a well-formed string the password and query option values are
// scrubbed out as whole tokens; for anything else the message is dropped and
// only the class kept, because a malformed string can surface anywhere in it.
export function describeConnectError(error: unknown, uri: string): string {
  if (!(error instanceof Error)) return 'unknown error';
  const parsed = parseMongoUri(uri);
  if (!parsed.wellFormed) return error.name;

  let message = error.message.split(uri).join('***');
  for (const fragment of parsed.secretFragments) {
    message = message.replace(new RegExp(`(?<![A-Za-z0-9._%-])${escapeRegExp(fragment)}(?![A-Za-z0-9._%-])`, 'gi'), '***');
  }
  return `${error.name}: ${message}`;
}
