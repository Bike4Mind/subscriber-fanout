// Loggable view of a MongoDB connection string. Only scheme, hosts and
// database are ever echoed, and only when they look like hosts and a database
// name; anything else is replaced by a placeholder, so a password with
// unencoded delimiters cannot land in the log by being mistaken for a host.
// The query string is dropped because its options can carry secrets too.

const HOST = /^(\[[0-9A-Fa-f:.]+\]|[A-Za-z0-9._-]+)(:\d{1,5})?$/;
const DATABASE = /^\/[A-Za-z0-9._-]*$/;

interface ParsedMongoUri {
  description: string;
  wellFormed: boolean;
  // Pieces of the password, raw and decoded, for scrubbing error messages.
  passwordFragments: string[];
}

function fragments(password: string): string[] {
  const out = new Set<string>();
  for (const piece of password.split(/[:@]/)) {
    if (!piece) continue;
    out.add(piece);
    try {
      out.add(decodeURIComponent(piece));
    } catch {
      // not percent-encoded; the raw piece is already in the set
    }
  }
  return [...out].sort((a, b) => b.length - a.length);
}

export function parseMongoUri(uri: string): ParsedMongoUri {
  const schemeEnd = uri.indexOf('://');
  if (schemeEnd === -1) {
    return { description: '<connection string without a scheme>', wellFormed: false, passwordFragments: [] };
  }

  const scheme = uri.slice(0, schemeEnd);
  const rest = uri.slice(schemeEnd + 3);
  const authorityEnd = rest.search(/[/?#]/);
  const authority = authorityEnd === -1 ? rest : rest.slice(0, authorityEnd);
  const tail = authorityEnd === -1 ? '' : rest.slice(authorityEnd);
  const at = authority.lastIndexOf('@');
  const userinfo = at === -1 ? '' : authority.slice(0, at);
  const hosts = authority.slice(at + 1);
  const pathEnd = tail.search(/[?#]/);
  const path = pathEnd === -1 ? tail : tail.slice(0, pathEnd);

  // An '@' after the authority means a credential with unencoded delimiters
  // was cut in half, so nothing after the cut can be trusted as a host.
  const wellFormed =
    hosts.length > 0 && hosts.split(',').every(host => HOST.test(host)) && !tail.includes('@');

  if (!wellFormed) {
    return {
      description: `${scheme}://<redacted: connection string not in the expected form>`,
      wellFormed: false,
      passwordFragments: [],
    };
  }

  const colon = userinfo.indexOf(':');
  return {
    description: `${scheme}://${at === -1 ? '' : '***:***@'}${hosts}${DATABASE.test(path) ? path : ''}`,
    wellFormed: true,
    passwordFragments: colon === -1 ? [] : fragments(userinfo.slice(colon + 1)),
  };
}

export function describeMongoUri(uri: string): string {
  return parseMongoUri(uri).description;
}

function escapeRegExp(text: string): string {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Driver and parser errors quote parts of the connection string in their
// messages. For a well-formed string the password is scrubbed out as whole
// tokens; for anything else the message is dropped and only the class kept,
// because a malformed string can surface anywhere in it.
export function describeConnectError(error: unknown, uri: string): string {
  if (!(error instanceof Error)) return 'unknown error';
  const parsed = parseMongoUri(uri);
  if (!parsed.wellFormed) return error.name;

  let message = error.message.split(uri).join('***');
  for (const fragment of parsed.passwordFragments) {
    message = message.replace(new RegExp(`(?<![A-Za-z0-9._%-])${escapeRegExp(fragment)}(?![A-Za-z0-9._%-])`, 'gi'), '***');
  }
  return `${error.name}: ${message}`;
}
