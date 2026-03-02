package com.kekwy.iarnet.example;

import com.kekwy.iarnet.api.Flow;
import com.kekwy.iarnet.api.sink.PrintSink;
import com.kekwy.iarnet.api.Resource;
import com.kekwy.iarnet.api.Workflow;
import com.kekwy.iarnet.api.function.Function;
import com.kekwy.iarnet.api.source.ConstantSource;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        Workflow wf = Workflow.create();
        Flow<String> input = wf.source(ConstantSource.of("Hello World! test1 test2 TTT"));
        Flow<String> words = input
                .flatMap(line -> Arrays.asList(line.split(" ")),
                        2, Resource.of(1.5, "1Gi"))
                .filter(w -> w.length() > 3)
                .map(String::toLowerCase)
                .map(Function.pythonMap(
                        "function/iarnet.py", "process", String.class, "function/requirements.txt"
                ));

//        Task checkpoint = wf.task("checkpoint", ctx -> {
//            // save state
//        });

        words.sink(PrintSink.of());
        wf.execute();
    }

}
