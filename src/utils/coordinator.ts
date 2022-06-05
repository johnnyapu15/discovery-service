import { COORDINATOR_CONNECTION, PUBLIC_HOSTNAME } from '@/config';
import { ACL, createClient, CreateMode, Event, Exception } from 'node-zookeeper-client'
import { promisify } from 'util';
import { logger } from './logger';

export const CoordinatorPaths = {
    test: 'test',
    /** server config */
    SERVER_PATH: 'AvailableServers',
    AVAILABLE_SERVERS: (publicHost: string) => `/${CoordinatorPaths.SERVER_PATH}/${publicHost}`,
    CURRENT_TOTAL_CONNECTIONS: (publicHost: string) => `/${CoordinatorPaths.SERVER_PATH}/${publicHost}/CurrentTotalConnections`,
    CURRENT_TOTAL_QUEUED_CONNECTIONS: (publicHost: string) => `/${CoordinatorPaths.SERVER_PATH}/${publicHost}/CurrentTotalQueuedConnections`,

    /** channel config */
    CHANNEL_PATH: 'Channels',
    AVAILABLE_CHANNELS: (channelId: string) => `/${CoordinatorPaths.CHANNEL_PATH}/${channelId}`,
    CURRENT_CHANNEL_CONNECTIONS: (channelId: string) => `/${CoordinatorPaths.CHANNEL_PATH}/${channelId}/CurrentConnections`,
    CURRENT_CHANNEL_QUEUED_CONNECTIONS: (channelId: string) => `/${CoordinatorPaths.CHANNEL_PATH}/${channelId}/CurrentQueuedConnections`,
}


const coordinator = createClient(
    COORDINATOR_CONNECTION
);


const InitializeService = () => {
    coordinator.once('connected', () => {
        console.info('coordinator connected.');
    });

    coordinator.connect();
};

type Watcher = (path: string) => void;
const Watch = (path: string, onGetData: (error: Error | Exception, data: Buffer) => void, onCreate: Watcher, onDataChanged: Watcher, onChildrenChanged: Watcher, onDeleted: Watcher, permanent = false) => {
    coordinator.getData(
        path
        , (evt) => {
            switch (evt.getType()) {
                case Event.NODE_CREATED:
                    if (onCreate) onCreate(evt.getPath());
                    break;
                case Event.NODE_DATA_CHANGED:
                    if (onDataChanged) onDataChanged(evt.getPath());
                    break;
                case Event.NODE_CHILDREN_CHANGED:
                    if (onChildrenChanged) onChildrenChanged(evt.getPath());
                    break;
                case Event.NODE_DELETED:
                    if (onDeleted) onDeleted(evt.getPath());
                    break;
            };

            if (permanent && evt.getType() !== Event.NODE_DELETED)
                Watch(path, onGetData, onCreate, onDataChanged, onChildrenChanged, onDeleted, permanent);
        }, (error, data, stat) => {
            onGetData(error, data);
        }
    );
}

interface Service {
    host: string,
    connections: number,
    queuedConnections: number,
}
const ServiceCoordinator = {
    Cache: new Map<string, Service>(),

    CreateServiceAsync: async () => {
        const promise = promisify(
            coordinator.transaction()
                .create(CoordinatorPaths.AVAILABLE_SERVERS(PUBLIC_HOSTNAME), CreateMode.EPHEMERAL)
                .create(CoordinatorPaths.CURRENT_TOTAL_CONNECTIONS(PUBLIC_HOSTNAME), CreateMode.EPHEMERAL, 0)
                .create(CoordinatorPaths.CURRENT_TOTAL_QUEUED_CONNECTIONS(PUBLIC_HOSTNAME), CreateMode.EPHEMERAL, 0)
                .commit
        ).bind(coordinator);
        return await promise();
    },

    GetAllServicesAsync: async () => {
        const promise = promisify<string, string[]>(coordinator.getChildren).bind(coordinator);
        return await promise(CoordinatorPaths.SERVER_PATH);
    },

    WatchService: (serviceHost: string) => {
        Watch(
            CoordinatorPaths.AVAILABLE_SERVERS(serviceHost),
            (err, data) => {
                if (err) {
                    logger.error(err)
                }
                const got = ServiceCoordinator.Cache.get(serviceHost);
                if (!got) {
                    ServiceCoordinator.Cache.set(serviceHost, { host: serviceHost, connections: 0, queuedConnections: 0 });
                }
            }
            , null, null, null,
            // onDelete
            (path) => {
                ServiceCoordinator.Cache.delete(serviceHost);
            }, true
        );
        // connection
        Watch(
            CoordinatorPaths.CURRENT_TOTAL_CONNECTIONS(serviceHost),

        )
    },

    WatchServices: () => {

    }
}

const ChannelCoordinator = {
    Cache: new Map(),

    CreateChannelAsync: async (channelId: string) => {
        const promise = promisify(
            coordinator.transaction()
                .create(CoordinatorPaths.AVAILABLE_CHANNELS(channelId), CreateMode.EPHEMERAL, Buffer.from(PUBLIC_HOSTNAME))
                .create(CoordinatorPaths.CURRENT_CHANNEL_CONNECTIONS(channelId), CreateMode.EPHEMERAL, Buffer.from('0'))
                .create(CoordinatorPaths.CURRENT_CHANNEL_QUEUED_CONNECTIONS(channelId), CreateMode.EPHEMERAL, Buffer.from('0'))
                .commit
        ).bind(coordinator);

        return await promise();
    },

    GetChannelAsync: async (channelId: string) => {
        const getData = promisify<string, Buffer>(coordinator.getData).bind(coordinator);
        const channelHostAsync = getData(CoordinatorPaths.AVAILABLE_CHANNELS(channelId));
        const connectionAsync = getData(CoordinatorPaths.CURRENT_CHANNEL_CONNECTIONS(channelId));
        const queuedAsync = getData(CoordinatorPaths.CURRENT_CHANNEL_QUEUED_CONNECTIONS(channelId));
        const got = await Promise.all([channelHostAsync, connectionAsync, queuedAsync]);
        return {
            channelHost: got[0].toString(),
            connections: got[1].toString(),
            queuedConnections: got[2].toString()
        };
    },

    UpdateChannelAsync: async (channelId: string, currentConnection: number, currentQueuedConnection: number) => {
        const promise = promisify(
            coordinator.transaction()
                .setData(CoordinatorPaths.CURRENT_CHANNEL_CONNECTIONS(channelId), Buffer.from(currentConnection.toString(10)))
                .setData(CoordinatorPaths.CURRENT_CHANNEL_QUEUED_CONNECTIONS(channelId), Buffer.from(currentQueuedConnection.toString(10)))

                .commit
        ).bind(coordinator);

        return await promise();
    },

    // 변량으로 채널 정보 업데이트하는 메소드 짜야 함. 변량의 기준은 캐시값으로.
    // 어차피 하나의 서버만 해당 채널에 대해 업데이트하므로 캐시 기준으로 해도 무방함.

    RemoveChannelAsync: async (channelId: string) => {
        const promise = promisify(
            coordinator.transaction()
                .remove(CoordinatorPaths.AVAILABLE_CHANNELS(channelId))
                .remove(CoordinatorPaths.CURRENT_CHANNEL_CONNECTIONS(channelId))
                .remove(CoordinatorPaths.CURRENT_CHANNEL_QUEUED_CONNECTIONS(channelId))
                .commit
        ).bind(coordinator);

        return await promise();
    },

}


const createNode = (path: string, data: Buffer, mode: number, callback: () => {}) => {
    coordinator.create(
        path, data, mode, (error) => {
            if (error) {
                logger.error('Failed to create node: %s due to: %s.', path, error);
            } else {
                console.log('Node: %s is successfully created.', path);
                callback();
            }
        }
    )
}


export { coordinator, InitializeService, ChannelCoordinator as ChannelManage };