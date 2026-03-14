package com.kekwy.iarnet.example;

import com.kekwy.iarnet.sdk.Flow;
import com.kekwy.iarnet.sdk.ExecutionConfig;
import com.kekwy.iarnet.sdk.Workflow;
import com.kekwy.iarnet.sdk.dsl.Inputs;
import com.kekwy.iarnet.sdk.dsl.Tasks;

/**
 * 智能交通视频分析流水线示例。
 *
 * <p>本示例展示多摄像头实时交通监控的典型 DAG 工作流：在边缘端完成帧采样与运动过滤，
 * 在云端执行 GPU 密集型目标检测与识别，最终汇合生成统一事件流并触发异常告警。
 * 该设计体现 IARNet 面向云边协同场景的分布式数据流执行能力。</p>
 *
 * <h2>论文支撑</h2>
 * <ul>
 *   <li><b>VideoStorm</b> (Zhang et al., NSDI 2017): 将视频分析建模为 DAG 流水线，
 *       研究资源-质量权衡与大规模调度，是视频分析系统架构的奠基性工作。</li>
 *   <li><b>YOLO</b> (Redmon et al., CVPR 2016): 实时目标检测的开创性论文，
 *       本示例检测阶段的算法基础。</li>
 *   <li><b>Neurosurgeon</b> (Kang et al., ASPLOS 2017): DNN 推理在云端与边缘之间的协同分割，
 *       为本示例中按资源声明划分边缘/云端计算提供理论依据。</li>
 * </ul>
 *
 * <p>各阶段处理逻辑采用 mock 实现，用于突出工作流 DSL 的表达能力。</p>
 */
public class Main {

    public static void main(String[] args) {
        Workflow workflow = Workflow.create("video-analytics");

        /*
         * 阶段 1：双路摄像头数据源
         */
        Flow<VideoFrame> cam1 = workflow.input("cam1-input", Inputs.of(
                new VideoFrame("cam1", 1, "frame1_data"),
                new VideoFrame("cam1", 2, "frame2_data"),
                new VideoFrame("cam1", 3, "frame3_data")
        ));
        Flow<VideoFrame> cam2 = workflow.input("cam2-input", Inputs.of(
                new VideoFrame("cam2", 1, "frame1_data"),
                new VideoFrame("cam2", 2, "frame2_data")
        ));

        /*
         * 阶段 2：边缘端帧解码（轻量资源）
         */
        ExecutionConfig edgeLight = ExecutionConfig.of()
                .replicas(1)
                .resource(b -> b.cpu(0.5).memory("256Mi").gpu(0));

        Flow<DecodedFrame> decoded1 = cam1.then("frame-decode-cam1", Main::decodeFrame, edgeLight);
        Flow<DecodedFrame> decoded2 = cam2.then("frame-decode-cam2", Main::decodeFrame, edgeLight);

        // TODO: 未实现
        // Flow<DecodedFrame> decoded1 = cam1.then(
        //         "frame-decode-cam1",
        //         Tasks.<VideoFrame, DecodedFrame>pythonTask("decode_frame"),
        //         edgeLight
        // );
        // Flow<DecodedFrame> decoded2 = cam2.then(
        //         "frame-decode-cam2",
        //         Tasks.<VideoFrame, DecodedFrame>pythonTask("decode_frame"),
        //         edgeLight
        // );

        /*
         * 阶段 3：多路摄像头汇合
         */
        Flow<DecodedFrame> mergedFrames = decoded1.join(
                "merge-cameras",
                decoded2,
                (a, b) -> a.or(() -> b)
                        .orElseThrow(() -> new IllegalStateException("join requires at least one input"))
        );

        /*
         * 阶段 4：运动过滤（仅保留有运动的帧，边缘侧）
         */
        Flow<DecodedFrame> motionFrames = mergedFrames
                .when(Main::hasMotion)
                .then("motion-filter", f -> f, edgeLight);

        /*
         * 阶段 5：目标检测（云端 GPU 密集型）
         */
        ExecutionConfig cloudGpu = ExecutionConfig.of()
                .replicas(2)
                .resource(b -> b.cpu(2).memory("4Gi").gpu(1));

        Flow<DetectionResult> detections = motionFrames.then(
                "object-detect",
                Main::detectObjects,
                cloudGpu
        );

        /*
         * 阶段 6：条件分流 - 行人重识别 vs 车辆号牌识别
         */
        ExecutionConfig cloudReid = ExecutionConfig.of()
                .replicas(1)
                .resource(b -> b.cpu(2).memory("2Gi").gpu(1));
        ExecutionConfig edgePlate = ExecutionConfig.of()
                .replicas(1)
                .resource(b -> b.cpu(1).memory("1Gi").gpu(0));

        Flow<PersonEvent> personEvents = detections
                .when(dr -> "person".equals(dr.objectType()))
                .then("person-reid", Main::reidentifyPerson, cloudReid)
                .then("person-to-event", Main::personToEvent);

        Flow<VehicleEvent> vehicleEvents = detections
                .when(dr -> "vehicle".equals(dr.objectType()))
                .then("plate-recognize", Main::recognizePlate, edgePlate)
                .then("vehicle-to-event", Main::vehicleToEvent);

        /*
         * 阶段 7：行人/车辆事件汇合为统一监控事件
         */
        Flow<MonitorEvent> allEvents = personEvents.join(
                "merge-events",
                vehicleEvents,
                (p, v) -> p.map(Main::personToMonitorEvent)
                        .or(() -> v.map(Main::vehicleToMonitorEvent))
                        .orElseThrow(() -> new IllegalStateException("join requires at least one input"))
        );

        /*
         * 阶段 8：归档所有事件，异常时触发告警
         */
        allEvents.then("archive-and-alert", event -> {
            archiveEvent(event);
            if (isAnomaly(event)) {
                sendAlert(generateAlert(event));
            }
        });

        workflow.execute();
    }

    // ======================== Mock 函数 ========================

    private static DecodedFrame decodeFrame(VideoFrame raw) {
        return new DecodedFrame(raw.cameraId(), raw.sequenceId(), raw.rawBytes() + "_decoded");
    }

    private static boolean hasMotion(DecodedFrame frame) {
        return frame.sequenceId() % 2 == 1;
    }

    private static DetectionResult detectObjects(DecodedFrame frame) {
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

    private static MonitorEvent personToMonitorEvent(PersonEvent p) {
        return new MonitorEvent(p.cameraId(), p.timestamp(), "person", p.identityId(), null);
    }

    private static MonitorEvent vehicleToMonitorEvent(VehicleEvent v) {
        return new MonitorEvent(v.cameraId(), v.timestamp(), "vehicle", null, v.plateNumber());
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

    /**
     * 原始视频帧（摄像头采集）。
     */
    private record VideoFrame(String cameraId, long sequenceId, String rawBytes) {
    }

    /**
     * 解码后的视频帧。
     */
    private record DecodedFrame(String cameraId, long sequenceId, String decodedData) {
    }

    /**
     * 目标检测结果（YOLO 等模型输出）。
     */
    private record DetectionResult(String cameraId, long sequenceId, String objectType, double confidence) {
    }

    /**
     * 行人重识别事件。
     */
    private record PersonEvent(String cameraId, long timestamp, String identityId) {
    }

    /**
     * 车辆号牌识别事件。
     */
    private record VehicleEvent(String cameraId, long timestamp, String plateNumber) {
    }

    /**
     * 统一监控事件（归档/告警用）。
     */
    private record MonitorEvent(
            String cameraId,
            long timestamp,
            String objectType,
            String identityId,
            String plateNumber
    ) {
    }
}
