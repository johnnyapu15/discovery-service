import { DiscoveredService } from "@/interfaces/discovery.interface";

class DiscoveryService {

    /**
     * 채널 생성이 가능한 가용 서버를 반환함
     */
    public async discoveryGenaratableService(): Promise<DiscoveredService> {
        return;
    }

    /**
     * channelId를 기반으로 coodinator에 적절한 service를 요청하고,
     * channelId에 해당하는 서버가 없으면 아무 가용 서버를 반환하고
     * 서버가 존재하고 가용할 경우 반환하고,
     * 서버가 존재하지만 비가용할 경우 에러를 반환함. (잠시 후 재시도 해주세요 등.)
     */
    public async discoveryAvailableService(channelId: string): Promise<DiscoveredService> {
        return;
    }



}