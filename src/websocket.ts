import { IMessageDataToClient } from './wireTypes.js';
import { ApiGatewayManagementApiClient, PostToConnectionCommand } from '@aws-sdk/client-apigatewaymanagementapi';
import { fromContainerMetadata, fromEnv, fromIni, fromInstanceMetadata } from '@aws-sdk/credential-providers';
import crypto from 'crypto';

const MAX_WS_PAYLOAD_BYTES = 120 * 1024;
const CHUNK_DATA_SIZE = 60 * 1024;

// API Gateway caps WebSocket frames at 128KB, so we chunk anything over 120KB.
// A direct WebSocket gateway (e.g. the self-host stack) has no such cap and
// forwards frames whole; set WEBSOCKET_DISABLE_CHUNKING=true there so clients
// receive un-chunked frames (the browser client does not reassemble chunks).
const DISABLE_CHUNKING = process.env.WEBSOCKET_DISABLE_CHUNKING === 'true';

export async function sendToConnection(
  connectionId: string,
  apiGatewayManagementClient: ApiGatewayManagementApiClient,
  action: IMessageDataToClient
) {
  const serialized = JSON.stringify(action);
  const byteLength = Buffer.byteLength(serialized, 'utf8');

  if (DISABLE_CHUNKING || byteLength <= MAX_WS_PAYLOAD_BYTES) {
    await apiGatewayManagementClient.send(
      new PostToConnectionCommand({
        ConnectionId: connectionId,
        Data: serialized,
      })
    );
    return;
  }

  const chunkId = crypto.randomUUID();
  const buf = Buffer.from(serialized, 'utf8');
  const chunks: string[] = [];
  let offset = 0;
  while (offset < buf.length) {
    let end = Math.min(offset + CHUNK_DATA_SIZE, buf.length);
    while (end > offset && (buf[end] & 0xc0) === 0x80) {
      end--;
    }
    chunks.push(buf.subarray(offset, end).toString('utf8'));
    offset = end;
  }
  for (let i = 0; i < chunks.length; i++) {
    const chunkPayload = JSON.stringify({
      _chunked: true,
      _chunkId: chunkId,
      _chunkIndex: i,
      _totalChunks: chunks.length,
      _data: chunks[i],
    });
    await apiGatewayManagementClient.send(
      new PostToConnectionCommand({
        ConnectionId: connectionId,
        Data: chunkPayload,
      })
    );
  }
}

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
