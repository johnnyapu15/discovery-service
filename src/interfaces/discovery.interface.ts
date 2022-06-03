export interface DiscoveredService {
    channelId: string;
    publicAddress: string;
    currentConnections: number;
    queuedConnections: number;
}