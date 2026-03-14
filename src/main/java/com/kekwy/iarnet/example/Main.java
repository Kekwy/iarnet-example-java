package com.kekwy.iarnet.example;

import com.kekwy.iarnet.sdk.ExecutionConfig;
import com.kekwy.iarnet.sdk.Flow;
import com.kekwy.iarnet.sdk.Workflow;
import com.kekwy.iarnet.sdk.type.TypeToken;

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
 * <p>各阶段处理逻辑采用 mock 实现，用于突出工作流 DSL 的表达能力。
 * 工作流输入（双路摄像头）在提交时由用户提供，不引入 Python 函数。</p>
 */
public class Main {

    public static void main(String[] args) {
        Workflow workflow = Workflow.create("video-analytics");

        /*
         * 阶段 1：双路摄像头数据源（输入在 execute 时提供）
         */
        Flow<DecodedFrame> decoded1 = workflow
                .input("cam1-input", new TypeToken<VideoFrame>() {})
                .then("frame-decode-cam1", Main::decodeFrame, edgeLight());
        Flow<DecodedFrame> decoded2 = workflow
                .input("cam2-input", new TypeToken<VideoFrame>() {})
                .then("frame-decode-cam2", Main::decodeFrame, edgeLight());

        /*
         * 阶段 2：多路摄像头汇合，产出包含两路上游结果的新类型
         */
        Flow<MergedFrames> mergedFrames = decoded1.combine(
                "merge-cameras",
                decoded2,
                (a, b) -> new MergedFrames(a.orElse(null), b.orElse(null))
        );

        /*
         * 阶段 3：运动过滤（仅保留有运动的帧，边缘侧）
         */
        Flow<MergedFrames> motionFrames = mergedFrames
                .when(Main::mergedHasMotion)
                .then("motion-filter", f -> f, edgeLight());

        /*
         * 阶段 4：目标检测（云端 GPU 密集型）
         */
        Flow<DetectionResult> detections = motionFrames.then(
                "object-detect",
                Main::detectObjectsFromMerged,
                cloudGpu()
        );

        /*
         * 阶段 5：条件分流 - 行人重识别 vs 车辆号牌识别
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
         * 阶段 6：行人/车辆事件汇合为包含两路上游结果的新类型，再转为统一监控事件
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

        /*
         * 阶段 7：归档所有事件，异常时触发告警
         */
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

    // ======================== Mock 函数 ========================

    private static DecodedFrame decodeFrame(VideoFrame raw) {
        return new DecodedFrame(raw.cameraId(), raw.sequenceId(), raw.rawBytes() + "_decoded");
    }

    private static boolean mergedHasMotion(MergedFrames m) {
        return (m.fromCam1() != null && m.fromCam1().sequenceId() % 2 == 1)
                || (m.fromCam2() != null && m.fromCam2().sequenceId() % 2 == 1);
    }

    private static DetectionResult detectObjectsFromMerged(MergedFrames m) {
        DecodedFrame frame = m.fromCam1() != null ? m.fromCam1() : m.fromCam2();
        if (frame == null) {
            throw new IllegalStateException("MergedFrames 至少应包含一路");
        }
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
     * 合并两路解码帧的结果（combine 阶段产出，包含两个上游任务结果）。
     */
    private record MergedFrames(DecodedFrame fromCam1, DecodedFrame fromCam2) {
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
     * 合并行人/车辆事件的结果（combine 阶段产出，包含两个上游任务结果）。
     */
    private record MergedEvents(PersonEvent person, VehicleEvent vehicle) {
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
