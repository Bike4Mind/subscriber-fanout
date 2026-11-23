import mongoose from 'mongoose';

const PARENTS = [
  ['sessionId', 'sessionmodels'], ['projectId', 'projects'], ['artifactId', 'artifacts'],
  ['fabFileId', 'fabfiles'], ['dataLakeId', 'datalakes'],
] as const;
const RESOURCE_COLLECTIONS = new Set([
  'sessionmodels', 'projects', 'artifacts', 'artifact_versions', 'artifact_contents',
  'fabfiles', 'fabfilechunks', 'quests', 'questmasterplans', 'publishedartifacts',
  'datalakes', 'datalakebatches', 'agents', 'skills', 'latticemodels',
]);

async function organizationIds(collection: string, id: unknown, seen = new Set<string>()): Promise<string[]> {
  const key = `${collection}:${String(id)}`;
  if (seen.has(key)) return [];
  seen.add(key);
  const ids: Array<string | mongoose.Types.ObjectId> = [String(id)];
  if (mongoose.isValidObjectId(id)) ids.push(new mongoose.Types.ObjectId(String(id)));
  const filter: Record<string, unknown> = { $or: [{ _id: { $in: ids } }, { id: String(id) }] };
  const row = await mongoose.connection.collection(collection).findOne(filter);
  if (!row) return [];
  const organizations = typeof row.organizationId === 'string' ? [row.organizationId] : [];
  if (row.tier === 'organization' && typeof row.scopeId === 'string') organizations.push(row.scopeId);
  if (row.tier === 'project' && row.scopeId) organizations.push(...await organizationIds('projects', row.scopeId, seen));
  for (const [field, parent] of PARENTS) {
    if (row[field]) organizations.push(...await organizationIds(parent, row[field], seen));
    if (row.source?.[field]) organizations.push(...await organizationIds(parent, row.source[field], seen));
  }
  return organizations;
}

export async function organizationMfaDeliveryAllowed(connectionId: string, collection: string, id: unknown): Promise<boolean> {
  if (!RESOURCE_COLLECTIONS.has(collection)) return true;
  if (!(await mongoose.connection.collection(collection).findOne({ _id: new mongoose.Types.ObjectId(String(id)) }, { projection: { _id: 1 } }))) return false;
  const organizations = await organizationIds(collection, id);
  if (!organizations.length) return true;
  const policies = await mongoose.connection.collection('organizations').find({
    _id: { $in: organizations.filter(value => mongoose.isValidObjectId(value)).map(value => new mongoose.Types.ObjectId(value)) },
    'mfaPolicy.required': true,
    deletedAt: null,
  }).toArray();
  const enforced = policies.some(org => {
    const deadline = org.mfaPolicy?.enforceAfter;
    return !deadline || !Number.isFinite(new Date(deadline).getTime()) || new Date(deadline).getTime() <= Date.now();
  });
  if (!enforced) return true;
  const connection = await mongoose.connection.collection('connections').findOne({ connectionId, endedAt: null });
  if (!connection?.userId || !mongoose.isValidObjectId(connection.userId)) return false;
  const user = await mongoose.connection.collection('users').findOne({ _id: new mongoose.Types.ObjectId(String(connection.userId)) }, { projection: { 'mfa.totpEnabled': 1 } });
  return user?.mfa?.totpEnabled === true;
}
