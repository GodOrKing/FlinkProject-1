package cn.itcast.streaming.function.window;

import cn.itcast.entity.ElectricFenceModel;
import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.datanucleus.state.LifeCycleState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * 对电子围栏进行自定义窗口操作，处理电子围栏判断逻辑
 */
public class ElectricFenceWindowFunction extends RichWindowFunction<ElectricFenceModel, ElectricFenceModel, String, TimeWindow> {
    //构建存储历史电子围栏数据的state对象
    private MapState<String, Integer> state = null;
    private String stateStartWith = "electricFence_";

    /**
     * 初始化资源
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //定义mapState的描述器（相当于表结构）
        MapStateDescriptor mapStateDescriptor = new MapStateDescriptor<String, Integer>(
                "ElectricFenceState",
                TypeInformation.of(new TypeHint<String>(){}),
                TypeInformation.of(new TypeHint<Integer>() { })
        );

        //不希望存储到state的历史的电子围栏状态数据持久存储
        //因此需要对flinkState的数据设置过期策略
        //获取全局的参数配置
        ParameterTool parameterTool = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Long timeOut  = Long.parseLong(parameterTool.getRequired("state.millionseconds"));

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.milliseconds(timeOut))//state的数据存活时间
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //配置Update为OnCreateAndWrite，表示每次数据更新的时候同时更新TTL Time
                //永远不返回过期的数据
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();

        //设置state的过期策略
        mapStateDescriptor.enableTimeToLive(ttlConfig);

        //初始化MapState的对象
        state = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    /**
     * 自定义窗口操作
     * @param key
     * @param timeWindow
     * @param iterable
     * @param collector
     * @throws Exception
     */
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<ElectricFenceModel> iterable, Collector<ElectricFenceModel> collector) throws Exception {
        //定义需要返回的电子围栏模型对象实体类
        ElectricFenceModel electricFenceModel = new ElectricFenceModel();

        //todo 1：对90s窗口的数据进行排序
        ArrayList<ElectricFenceModel> fenceModelArrayList = Lists.newArrayList(iterable);
        Collections.sort(fenceModelArrayList);
        System.out.println("当前滚动窗口的windowFunction，输入的数据长度为："+fenceModelArrayList.size());

        //todo 2：从state中获取车辆vin对应的flag标记（车辆上一次窗口是否在电子围栏中）0：电子围栏内 1：电子围栏外
        Integer lastStateValue = state.get(stateStartWith + key);
        System.out.println("上一个窗口的电子围栏状态是："+ lastStateValue);
        if(lastStateValue == null){
            lastStateValue = -999999;
        }

        //todo 3：判断当前处于电子围栏内还是电子围栏外
        //定义当前车辆在电子围栏内出现的次数
        long electricFenceInCount = fenceModelArrayList.stream().filter(efModel -> (efModel.getNowStatus() == 0)).count();
        //定义当前车辆在电子围栏外出现的次数
        long electricFenceOutCount = fenceModelArrayList.stream().filter(efModel -> (efModel.getNowStatus() == 1)).count();

        //定义当前窗口的电子围栏状态
        int currentStateValue = 1;
        //90s内车辆出现在电子围栏内的次数多于出现在电子围栏外的次数，则认为当前处于电子围栏内
        if(electricFenceInCount >= electricFenceOutCount) currentStateValue = 0;

        //todo 4：将当前窗口的电子围栏状态写入到state中，供下一次判断上一次状态
        state.put(stateStartWith + key, currentStateValue);

        //todo 5：如果当前电子围栏状态与上一次电子围栏状态不同
        //如果上一次窗口处于电子围栏内，而本次是电子围栏外，则将出电子围栏的时间写入到数据库中
        //如果上一次窗口处于电子围栏外，而本次是电子围栏内，则将进入电子围栏的时间写入到数据库中
        if(lastStateValue != currentStateValue){
            //如果上一次窗口处于电子围栏外，而本次是电子围栏内，则将进入电子围栏的时间写入到数据库中
            if((lastStateValue == 1 || lastStateValue == -999999) && currentStateValue == 0){
                //需要在数据库中添加一条进入电子围栏的记录
                ElectricFenceModel fenceModel = fenceModelArrayList.stream().filter(efModel -> efModel.getNowStatus() == 0).findFirst().get();
                BeanUtils.copyProperties(electricFenceModel, fenceModel);
                //将终端时间赋值给对象作为进入时间
                electricFenceModel.setInEleTime(fenceModel.getTerminalTime());
                //状态报警 0：出围栏 1：进围栏
                electricFenceModel.setStatusAlarm(1);
                //返回数据
                collector.collect(electricFenceModel);
            }else if((lastStateValue == 0 || lastStateValue == -999999) && currentStateValue ==1){
                //需要在数据库中补充上上一次窗口的出电子围栏的时间
                ElectricFenceModel fenceModel = fenceModelArrayList.stream().filter(efModel -> efModel.getNowStatus() == 1).sorted(Comparator.reverseOrder()).findFirst().get();
                BeanUtils.copyProperties(electricFenceModel, fenceModel);
                //将终端时间赋值给对象作为出电子围栏时间
                electricFenceModel.setOutEleTime(fenceModel.getTerminalTime());
                //状态报警 0：出围栏 1：进围栏
                electricFenceModel.setStatusAlarm(0);
                collector.collect(electricFenceModel);
            }
        }
    }
}
