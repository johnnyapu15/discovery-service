import { COORDINATOR_CONNECTION } from '@/config';
import { ACL, createClient, CreateMode } from 'node-zookeeper-client'
import { logger } from './logger';


const coordinator = createClient(
    COORDINATOR_CONNECTION
);

coordinator.once('connected', () => {
    logger.info('Coordinator connected.');
});

const createNode = (path: string, data: Buffer, mode: number, callback: () => {}) => {
    coordinator.create(
        path, data, mode, (error) => {
            if (error) {
                logger.error('Failed to create node: %s due to: %s.', path, error);
            } else {
                //console.log('Node: %s is successfully created.', path);
                callback();
            }
        }
    )
}

coordinator.connect();

export { coordinator };