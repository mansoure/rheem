package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

/**
 * Provides a {@link Collection} to a Spark job.
 */
public class SparkTableSource  extends TableSource implements Serializable, SparkExecutionOperator {

    public SparkTableSource(){
        super();
    }
    public SparkTableSource(String inputUrl, String delimiter) {

        super(inputUrl,delimiter, getColumnNames(inputUrl));

    }

    public SparkTableSource(String inputUrl) {

        super(inputUrl, getColumnNames(inputUrl));

    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkTableSource(TableSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        final JavaRDD<String> lineRdd = sparkExecutor.sc.textFile(this.getInputUrl());

//        Function2 mapFunc = new Function2<Integer, Iterator<String>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer index, Iterator<String> stringIterator) {
//                if (index==0 && stringIterator.hasNext()) {
//                    stringIterator.next();
//                    return stringIterator;
//                }
//                else {
//                    return stringIterator;
//                }
//            }
//        };
        final JavaRDD<Record> records = lineRdd
                //.filter(r-> r!=lineRdd.first()) // skip the header line
                //.mapPartitionsWithIndex((Function2<Integer, Iterator<String>, Iterator<String>>)mapFunc, false)
                .mapPartitionsWithIndex(
                        new Function2<Integer, Iterator<String>, Iterator<String>>() {
                            public Iterator<String> call(Integer index, Iterator<String> stringIterator) {
                                if (index==0 && stringIterator.hasNext()) {
                                     stringIterator.next();
                                    return stringIterator;
                            }
                            else {
                            return stringIterator;
                            }
                    }
                }, false)
                .map(r -> new Record(r.split(delimiter))); // split into a record

        this.name(records);
        output.accept(records, sparkExecutor);

        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.spark.tablesource.load.prepare", sparkExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "rheem.spark.tablesource.load.main", sparkExecutor.getConfiguration()
        ));
        output.getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkTableSource(this.getInputUrl());
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.spark.tablesource.load.prepare", "rheem.spark.tablesource.load.main");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
