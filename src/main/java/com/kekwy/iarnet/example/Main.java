package com.kekwy.iarnet.example;

import com.kekwy.iarnet.sdk.ExecutionConfig;
import com.kekwy.iarnet.sdk.Flow;
import com.kekwy.iarnet.sdk.Workflow;
import com.kekwy.iarnet.sdk.type.TypeToken;

/**
 * 工作流调用模型示例：单入口，输入到达后开始执行。
 *
 * <p>与「双数据源、持续数据流」不同，本示例表达的是：每次向工作流提交一条输入（如一帧视频），
 * 该输入从入口节点进入，沿 DAG 流动；条件分支可能只命中一侧，Combine 会等两路都到齐（或空响应）后汇聚，
 * 最终产生一次输出。适合请求/任务驱动的云边协同场景。</p>
 *
 * <p>流水线结构：</p>
 * <pre>
 *   input(frame) → decode → motion-filter → object-detect
 *                                              ↓
 *                        ┌────────────────────┼────────────────────┐
 *                        ↓ (person)          ↓ (vehicle)          │
 *                   person-reid         plate-recognize            │
 *                        ↓                    ↓                    │
 *                   person-to-event    vehicle-to-event           │
 *                        └────────── combine ──────────┘          │
 *                                    ↓                             │
 *                            to-monitor-event → archive-and-alert  │
 * </pre>
 */
public class Main {

    public static void main(String[] args) {
        Workflow workflow = Workflow.create("video-analytics");

        /*
         * 单入口：一帧输入到达后，从 decode 开始执行。
         */
        Flow<DecodedFrame> decoded = workflow
                .input("frame", new TypeToken<VideoFrame>() {})
                .then("frame-decode", Main::decodeFrame, edgeLight());

        /*
         * 运动过滤（条件：有运动才往下传）。
         */
        Flow<DecodedFrame> motionFrames = decoded
                .when(Main::decodedHasMotion)
                .then("motion-filter", f -> f, edgeLight());

        /*
         * 目标检测。
         */
        Flow<DetectionResult> detections = motionFrames.then(
                "object-detect",
                Main::detectFromFrame,
                cloudGpu()
        );

        /*
         * 条件分流：行人分支 / 车辆分支（同一 execution 可能只走一侧，未走的一侧发空，Combine 仍可立即汇聚）。
         */
        Flow<PersonEvent> personEvents = detections
                .when(dr -> "person".equals(dr.objectType()))
                .then("person-reid", Main::reidentifyPerson, cloudReid())
                .then("person-to-event", Main::personToEvent);

        Flow<VehicleEvent> vehicleEvents = detections
                .when(dr -> "vehicle".equals(dr.objectType()))
                .then("plate-recognize", Main::recognizePlate, edgePlate())
                .then("vehicle-to-event", Main::vehicleToEvent);

        /*
         * 两路汇聚（Optional 表示某路可能为空），再转为统一监控事件并归档/告警。
         */
        Flow<MergedEvents> combinedEvents = personEvents.combine(
                "merge-events",
                vehicleEvents,
                (p, v) -> new MergedEvents(p.orElse(null), v.orElse(null))
        );

        Flow<MonitorEvent> allEvents = combinedEvents.then(
                "to-monitor-event",
                Main::mergedToMonitorEvent
        );

        allEvents.then("archive-and-alert", event -> {
            archiveEvent(event);
            if (isAnomaly(event)) {
                sendAlert(generateAlert(event));
            }
        });

        workflow.execute();
    }

    private static ExecutionConfig edgeLight() {
        return ExecutionConfig.of()
                .replicas(1)
                .resource(b -> b.cpu(0.5).memory("256Mi").gpu(0));
    }

    private static ExecutionConfig cloudGpu() {
        return ExecutionConfig.of()
                .replicas(2)
                .resource(b -> b.cpu(2).memory("4Gi").gpu(1));
    }

    private static ExecutionConfig cloudReid() {
        return ExecutionConfig.of()
                .replicas(1)
                .resource(b -> b.cpu(2).memory("2Gi").gpu(1));
    }

    private static ExecutionConfig edgePlate() {
        return ExecutionConfig.of()
                .replicas(1)
                .resource(b -> b.cpu(1).memory("1Gi").gpu(0));
    }

    // ======================== 节点函数（Mock） ========================

    private static DecodedFrame decodeFrame(VideoFrame raw) {
        return new DecodedFrame(raw.cameraId(), raw.sequenceId(), raw.rawBytes() + "_decoded");
    }

    private static boolean decodedHasMotion(DecodedFrame f) {
        return f.sequenceId() % 2 == 1;
    }

    private static DetectionResult detectFromFrame(DecodedFrame frame) {
        return new DetectionResult(
                frame.cameraId(),
                frame.sequenceId(),
                frame.sequenceId() % 2 == 0 ? "person" : "vehicle",
                0.95
        );
    }

    private static PersonEvent reidentifyPerson(DetectionResult dr) {
        return new PersonEvent(dr.cameraId(), dr.sequenceId(), "pid_" + dr.sequenceId());
    }

    private static PersonEvent personToEvent(PersonEvent pe) {
        return pe;
    }

    private static VehicleEvent recognizePlate(DetectionResult dr) {
        return new VehicleEvent(dr.cameraId(), dr.sequenceId(), "京A" + dr.sequenceId());
    }

    private static VehicleEvent vehicleToEvent(VehicleEvent ve) {
        return ve;
    }

    private static MonitorEvent mergedToMonitorEvent(MergedEvents e) {
        if (e.person() != null) {
            return new MonitorEvent(
                    e.person().cameraId(), e.person().timestamp(),
                    "person", e.person().identityId(), null
            );
        }
        if (e.vehicle() != null) {
            return new MonitorEvent(
                    e.vehicle().cameraId(), e.vehicle().timestamp(),
                    "vehicle", null, e.vehicle().plateNumber()
            );
        }
        throw new IllegalStateException("MergedEvents 至少应包含一路");
    }

    private static void archiveEvent(MonitorEvent event) {
        System.out.println("[ARCHIVE] " + event);
    }

    private static boolean isAnomaly(MonitorEvent event) {
        return "vehicle".equals(event.objectType()) && event.plateNumber() != null
                && event.plateNumber().contains("3");
    }

    private static String generateAlert(MonitorEvent event) {
        return "ALERT: anomaly detected - " + event;
    }

    private static void sendAlert(String alert) {
        System.out.println(alert);
    }

    // ======================== 数据类型 ========================

    private record VideoFrame(String cameraId, long sequenceId, String rawBytes) {
    }

    private record DecodedFrame(String cameraId, long sequenceId, String decodedData) {
    }

    private record DetectionResult(String cameraId, long sequenceId, String objectType, double confidence) {
    }

    private record PersonEvent(String cameraId, long timestamp, String identityId) {
    }

    private record VehicleEvent(String cameraId, long timestamp, String plateNumber) {
    }

    private record MergedEvents(PersonEvent person, VehicleEvent vehicle) {
    }

    private record MonitorEvent(
            String cameraId,
            long timestamp,
            String objectType,
            String identityId,
            String plateNumber
    ) {
    }
}
