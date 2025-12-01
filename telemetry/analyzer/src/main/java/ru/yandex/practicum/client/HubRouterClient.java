package ru.yandex.practicum.client;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.model.Action;

import java.time.Instant;

@Slf4j
@Service
public class HubRouterClient {
    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouter;

    public void sendAction(Action action) {
        if (action == null || action.getScenario() == null || action.getSensor() == null) {
            log.warn("Action, сценарий или сенсор равны null. Действие не отправлено.");
            return;
        }
        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(action.getScenario().getHubId())
                .setScenarioName(action.getScenario().getName())
                .setAction(toDeviceActionProto(action))
                .setTimestamp(timestamp)
                .build();

        try {
            hubRouter.handleDeviceAction(request);
            log.info("Отправлена команда на хаб {} для сценария {}",
                    action.getScenario().getHubId(), action.getScenario().getName());
        } catch (Exception e) {
            log.error("Ошибка при отправке команды на Hub Router: {}", e.getMessage(), e);
        }
    }


    private DeviceActionProto toDeviceActionProto(Action action) {
        ActionTypeProto actionTypeProto = switch (action.getType()) {
            case ACTIVATE -> ActionTypeProto.ACTIVATE;
            case DEACTIVATE -> ActionTypeProto.DEACTIVATE;
            case INVERSE -> ActionTypeProto.INVERSE;
            case SET_VALUE -> ActionTypeProto.SET_VALUE;
        };

        DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                .setSensorId(action.getSensor().getId())
                .setType(actionTypeProto);

        if (action.getValue() != null) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }
}

