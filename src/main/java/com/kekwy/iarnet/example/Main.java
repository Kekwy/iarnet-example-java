package com.kekwy.iarnet.example;

import com.kekwy.iarnet.sdk.BranchedFlow;
import com.kekwy.iarnet.sdk.Flow;
import com.kekwy.iarnet.sdk.Resource;
import com.kekwy.iarnet.sdk.Workflow;
import com.kekwy.iarnet.sdk.sink.PrintSink;
import com.kekwy.iarnet.sdk.source.ConstantSource;

import java.time.Duration;
import java.util.List;
import java.util.Locale;

/**
 * 面向 CTR 场景的训练样本构造与训练流程示例。
 *
 * <p>该示例用于展示工作流 DSL 对典型智能应用阶段结构的表达能力，
 * 包括：行为日志输入、数据清洗、正负样本构造、用户/物品特征关联、
 * 训练样本生成、批处理、训练以及评估等阶段。</p>
 *
 * <p>为突出流程表达能力，示例中的训练与评估逻辑采用 mock 方式实现，
 * 不对应真实工业级 CTR 模型。</p>
 */
public class Main {

    public static void main(String[] args) {
        Workflow workflow = Workflow.create();

        /*
         * 阶段 1：输入数据源
         * - 用户行为日志
         * - 用户画像
         * - 物品画像
         */
        Flow<BehaviorEvent> behaviorEvents = workflow.source(ConstantSource.of(
                new BehaviorEvent("u1", "i1", 1_000L, "view", "app"),
                new BehaviorEvent("u1", "i1", 1_200L, "click", "app"),
                new BehaviorEvent("u2", "i2", 1_500L, "view", "web"),
                new BehaviorEvent("u2", "i3", 1_800L, "click", "web"),
                new BehaviorEvent("u3", "i2", 2_100L, "view", "app"),
                new BehaviorEvent("u3", "i4", 2_300L, "view", "app")
        ));

        Flow<UserProfile> userProfiles = workflow.source(ConstantSource.of(
                new UserProfile("u1", 24, "east", true),
                new UserProfile("u2", 32, "west", false),
                new UserProfile("u3", 28, "north", false)
        ));

        Flow<ItemProfile> itemProfiles = workflow.source(ConstantSource.of(
                new ItemProfile("i1", "shoes", 199.0),
                new ItemProfile("i2", "book", 39.0),
                new ItemProfile("i3", "bag", 499.0),
                new ItemProfile("i4", "watch", 1299.0)
        ));

        /*
         * 阶段 2：数据清洗与规范化
         */
        Flow<BehaviorEvent> cleanedEvents = behaviorEvents
                .filter(Main::isValidEvent)
                .map(Main::normalizeEvent, 2, Resource.of(1.5, "1Gi"));

        /*
         * 阶段 3：正负样本构造
         * - click 事件直接作为正样本
         * - view 事件经过简化负采样后作为负样本
         */
        BranchedFlow<BehaviorEvent> labeledBranches =
                cleanedEvents.branch(event -> "click".equals(event.eventType()) ? 0 : 1);

        Flow<LabeledEvent> positiveSamples = labeledBranches.getFlow(0)
                .map(event -> new LabeledEvent(event.userId(), event.itemId(), event.ts(), 1));

        Flow<LabeledEvent> negativeSamples = labeledBranches.getFlow(1)
                .filter(Main::selectAsNegativeSample)
                .map(event -> new LabeledEvent(event.userId(), event.itemId(), event.ts(), 0));

        Flow<LabeledEvent> labeledSamples = positiveSamples.union(negativeSamples);

        /*
         * 可选观察节点：统计每个用户参与构造的训练样本数量
         */
        labeledSamples.keyBy(LabeledEvent::userId)
                .fold(0, (count, ignored) -> count + 1)
                .map(count -> "sample_count_per_user=" + count)
                .sink(PrintSink.of());

        /*
         * 阶段 4：用户侧特征关联
         */
        Flow<UserEnrichedSample> userEnrichedSamples = labeledSamples
                .keyBy(LabeledEvent::userId)
                .join(
                        userProfiles.keyBy(UserProfile::userId),
                        Duration.ofSeconds(10),
                        (sample, user) -> new UserEnrichedSample(
                                sample.userId(),
                                sample.itemId(),
                                sample.ts(),
                                sample.label(),
                                user.age(),
                                user.region(),
                                user.vip()
                        )
                );

        /*
         * 阶段 5：物品侧特征关联
         */
        Flow<ItemEnrichedSample> fullyEnrichedSamples = userEnrichedSamples
                .keyBy(UserEnrichedSample::itemId)
                .join(
                        itemProfiles.keyBy(ItemProfile::itemId),
                        Duration.ofSeconds(10),
                        (sample, item) -> new ItemEnrichedSample(
                                sample.userId(),
                                sample.itemId(),
                                sample.ts(),
                                sample.label(),
                                sample.age(),
                                sample.region(),
                                sample.vip(),
                                item.category(),
                                item.price()
                        )
                );

        /*
         * 阶段 6：训练样本生成
         */
        Flow<TrainingSample> trainingSamples = fullyEnrichedSamples
                .map(Main::buildTrainingSample);

        /*
         * 阶段 7：批处理
         * 使用 fold 算子实现每 3 个样本聚合为一个批次
         */
        Flow<List<TrainingSample>> trainingBatches = trainingSamples
                .keyBy(sample -> 0) // 分区到单一节点
                .fold(new BatchAccumulator(), Duration.ofHours(1), (acc, sample) -> {
                    acc.buffer.add(sample);
                    if (acc.buffer.size() >= 3) {
                        acc.readyBatch = new java.util.ArrayList<>(acc.buffer);
                        acc.buffer.clear();
                    } else {
                        acc.readyBatch = null;
                    }
                    return acc;
                })
                .map(acc -> acc.readyBatch)
                .filter(batch -> batch != null);

        /*
         * 阶段 8：模型训练（mock）
         */
        Flow<MockTrainingResult> trainingResults = trainingBatches
                .map(Main::mockTrainBatch);

        /*
         * 阶段 9：模型评估（mock）
         */
        trainingResults
                .map(Main::mockEvaluate)
                .sink(PrintSink.of());

        workflow.execute();
    }

    /**
     * 判断行为日志是否有效。
     */
    private static boolean isValidEvent(BehaviorEvent event) {
        return event.userId() != null
                && !event.userId().isBlank()
                && event.itemId() != null
                && !event.itemId().isBlank();
    }

    /**
     * 对原始行为日志进行规范化处理。
     */
    private static BehaviorEvent normalizeEvent(BehaviorEvent event) {
        String normalizedType = event.eventType() == null
                ? "view"
                : event.eventType().trim().toLowerCase(Locale.ROOT);

        String normalizedPlatform = event.platform() == null
                ? "unknown"
                : event.platform().trim().toLowerCase(Locale.ROOT);

        return new BehaviorEvent(
                event.userId(),
                event.itemId(),
                event.ts(),
                normalizedType,
                normalizedPlatform
        );
    }

    /**
     * 简化的负样本筛选逻辑。
     *
     * <p>真实 CTR 场景中负样本通常来自曝光未点击事件或经专门负采样策略构造。
     * 本示例仅为突出工作流结构，采用确定性规则进行 mock。</p>
     */
    private static boolean selectAsNegativeSample(BehaviorEvent event) {
        return Math.abs(event.itemId().hashCode()) % 2 == 0;
    }

    /**
     * 将富化后的样本转换为训练样本。
     */
    private static TrainingSample buildTrainingSample(ItemEnrichedSample sample) {
        String featureVector =
                "age=" + sample.age()
                        + ",region=" + sample.region()
                        + ",vip=" + sample.vip()
                        + ",category=" + sample.category()
                        + ",price=" + sample.price();

        return new TrainingSample(
                sample.userId(),
                sample.itemId(),
                sample.label(),
                featureVector
        );
    }

    /**
     * mock 训练步骤：将一个 batch 转换为训练结果摘要。
     */
    private static MockTrainingResult mockTrainBatch(List<TrainingSample> batch) {
        int positiveCount = 0;
        for (TrainingSample sample : batch) {
            if (sample.label() == 1) {
                positiveCount++;
            }
        }

        double mockLoss = 1.0 / (batch.size() + positiveCount + 1.0);

        return new MockTrainingResult(
                batch.size(),
                positiveCount,
                mockLoss,
                batch.get(0)
        );
    }

    /**
     * mock 评估步骤：基于训练结果生成可观察指标。
     */
    private static String mockEvaluate(MockTrainingResult result) {
        double mockAuc = Math.min(0.95, 0.60 + result.positiveCount() * 0.05);

        return "MockMetric{"
                + "batchSize=" + result.batchSize()
                + ", positiveCount=" + result.positiveCount()
                + ", loss=" + String.format(Locale.ROOT, "%.4f", result.loss())
                + ", auc=" + String.format(Locale.ROOT, "%.4f", mockAuc)
                + ", firstSample=" + result.firstSample()
                + "}";
    }

    /**
     * 用户行为日志。
     */
    private record BehaviorEvent(
            String userId,
            String itemId,
            long ts,
            String eventType,
            String platform
    ) {
    }

    /**
     * 用户画像。
     */
    private record UserProfile(
            String userId,
            int age,
            String region,
            boolean vip
    ) {
    }

    /**
     * 物品画像。
     */
    private record ItemProfile(
            String itemId,
            String category,
            double price
    ) {
    }

    /**
     * 带标签的样本。
     */
    private record LabeledEvent(
            String userId,
            String itemId,
            long ts,
            int label
    ) {
    }

    /**
     * 关联用户画像后的样本。
     */
    private record UserEnrichedSample(
            String userId,
            String itemId,
            long ts,
            int label,
            int age,
            String region,
            boolean vip
    ) {
    }

    /**
     * 同时关联用户画像与物品画像后的样本。
     */
    private record ItemEnrichedSample(
            String userId,
            String itemId,
            long ts,
            int label,
            int age,
            String region,
            boolean vip,
            String category,
            double price
    ) {
    }

    /**
     * 训练样本。
     */
    private record TrainingSample(
            String userId,
            String itemId,
            int label,
            String featureVector
    ) {
    }

    /**
     * mock 训练结果摘要。
     */
    private record MockTrainingResult(
            int batchSize,
            int positiveCount,
            double loss,
            TrainingSample firstSample
    ) {
    }

    /**
     * 批处理累加器。
     */
    public static class BatchAccumulator {
        public List<TrainingSample> buffer = new java.util.ArrayList<>();
        public List<TrainingSample> readyBatch = null;
    }
}