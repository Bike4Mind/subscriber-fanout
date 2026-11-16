import { fromContainerMetadata, fromEnv, fromIni, fromInstanceMetadata } from '@aws-sdk/credential-providers';

export const credentialProviderChain = async () => {
  // Try ECS container metadata (Fargate environment)
  const ecsCreds = fromContainerMetadata();
  const ecs = await ecsCreds().catch(() => null);
  if (ecs?.accessKeyId) return { source: 'ecs', credentials: ecs };

  // Try EC2 instance metadata (if applicable)
  const instanceCreds = fromInstanceMetadata();
  const instance = await instanceCreds().catch(() => null);
  if (instance?.accessKeyId) return { source: 'instance', credentials: instance };

  // Try environment variables next
  const envCreds = fromEnv();
  const env = await envCreds();
  if (env.accessKeyId && env.secretAccessKey) return { source: 'env', credentials: env };

  // Last try shared credentials file - this is common in development
  const iniCreds = fromIni();
  const ini = await iniCreds().catch(() => null);
  if (ini?.accessKeyId) return { source: 'ini', credentials: ini };

  throw new Error('No valid AWS credentials found in the chain');
};
