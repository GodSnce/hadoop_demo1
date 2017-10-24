package com.lsl.bigdata.mr.flowsum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by lishanglai on 2017/10/23.
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long upFlow;
    private long dFlow;
    private long sumFlow;

    //发序列化时，需要反射调用空参构造函数，所以要显示定义一个
    public FlowBean() {}

    public FlowBean(long upFlow, long dFlow) {
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow + dFlow;
    }

    public void set(long upFlow,long dFlow){
        this.upFlow = upFlow;
        this.dFlow = dFlow;
        this.sumFlow = upFlow + dFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getdFlow() {
        return dFlow;
    }

    public void setdFlow(long dFlow) {
        this.dFlow = dFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(dFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * 注意：反序列化的顺序跟序列化的顺序完全一致
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {

        upFlow = in.readLong();
        dFlow = in.readLong();
        sumFlow = in.readLong();

    }

    /**
     * 重写tostring
     * @return
     */
    @Override
    public String toString() {
        return upFlow + "\t" + dFlow + "\t" + sumFlow;
    }

    /**
     * 从大到小，当前对象和要比较的对象比，如果当前对象大，返回-1，交换他们的位置
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {

        return this.sumFlow>o.getSumFlow()?-1:1;
    }
}
