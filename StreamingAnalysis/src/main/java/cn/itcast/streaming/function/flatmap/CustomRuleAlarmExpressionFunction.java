package cn.itcast.streaming.function.flatmap;

import cn.itcast.entity.CustomRuleAlarmResultModel;
import cn.itcast.entity.ItcastDataPartObj;
import cn.itcast.entity.RuleInfoModel;
import cn.itcast.entity.VehicleInfoModel;
import cn.itcast.utils.CustomRuleUtil;
import com.google.common.collect.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Objects;

/**
 * 自定义告警规则合并数据流的业务逻辑
 * 将自定义告警规则应用到数据流中
 */
public class CustomRuleAlarmExpressionFunction implements CoFlatMapFunction<
        ArrayList<Tuple2<ItcastDataPartObj, VehicleInfoModel>>,
        ArrayList<RuleInfoModel>,
        ArrayList<CustomRuleAlarmResultModel>> {
    //定义全局变量接收数据流的数据
    ArrayList<RuleInfoModel> exprssionList = new ArrayList<>();

    /**
     * 第一个窗口数据流的操作方法
     * @param value
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap1(ArrayList<Tuple2<ItcastDataPartObj, VehicleInfoModel>> value, Collector<ArrayList<CustomRuleAlarmResultModel>> collector) throws Exception {
        //定义返回的列表对象
        ArrayList<CustomRuleAlarmResultModel> resultModelArrayList = new ArrayList<>();

        //todo 1：遍历集合对象中的每条数据（遍历的是每个自定义规则对象）,多对多的关系，需要将每个规则与每条数据进行判断处理
        //可能查询的到自定义规则有多条数据，因此需要判断当前窗口的每条数据是否符合规则列表中的任意规则
        for (RuleInfoModel ruleInfoModel: exprssionList) {
            //todo 2：遍历窗口中的每条数据，判断是否匹配某个告警规则
            for (Tuple2<ItcastDataPartObj, VehicleInfoModel> tuple2 : value) {
                //获取到原始数据
                ItcastDataPartObj itcastDataPartObj = tuple2.f0;
                //获取到车辆基础信息数据
                VehicleInfoModel vehicleInfoModel = tuple2.f1;

                //todo 3：定义返回自定义告警规则对象
                CustomRuleAlarmResultModel ruleAlarmResultModel = null;

                //todo 4：判断指定的字段名是否在javaBean对象中存在该属性
                //如果第三个字段存在，意味着存在两个计算表达式（此时不需要判断第四个字段是否存在，因为业务中约定了自定义告警规则的字段是2-4个字段，
                // 而表达式工具类处理了第四个字段为空的情况）
                if (isExistsField(itcastDataPartObj, ruleInfoModel.getAlarmParam3Field())) {
                    //这是两个表达式的场景
                    //是否存在第四个字段
                    boolean existsField4 = isExistsField(itcastDataPartObj, ruleInfoModel.getAlarmParam4Field());
                    boolean expressionResult1 = CustomRuleUtil.expression(
                            Double.parseDouble(BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam1Field())),
                            Double.parseDouble(BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam2Field())),
                            ruleInfoModel.getOperator1(),
                            ruleInfoModel.getAlarmThreshold1(),
                            ruleInfoModel.getRuleSymbol1(),
                            ruleInfoModel.getAlarmThreshold2(),
                            ruleInfoModel.getRuleSymbol2(),
                            ruleInfoModel.getLogicalSymbol()
                    );

                    boolean expressionResult2 = false;
                    //判断第四个字段是否为空
                    if(existsField4) {
                        expressionResult2 = CustomRuleUtil.expression(
                                Double.parseDouble(BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam3Field())),
                                Double.parseDouble(BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam4Field())),
                                ruleInfoModel.getOperator1(),
                                ruleInfoModel.getAlarmThreshold1(),
                                ruleInfoModel.getRuleSymbol1(),
                                ruleInfoModel.getAlarmThreshold2(),
                                ruleInfoModel.getRuleSymbol2(),
                                ruleInfoModel.getLogicalSymbol()
                        );
                    }else{
                        //假如第四个字段不存在（空或者写错了），第三个和第四个字段之间的逻辑运算符就没必要写了
                        expressionResult2 = CustomRuleUtil.expression(
                                Double.parseDouble(BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam3Field())),
                                0,
                                ruleInfoModel.getOperator1(),
                                ruleInfoModel.getAlarmThreshold1(),
                                ruleInfoModel.getRuleSymbol1(),
                                ruleInfoModel.getAlarmThreshold2(),
                                ruleInfoModel.getRuleSymbol2(),
                                null        //逻辑运算符设置为空
                        );
                    }
                    System.out.println("第一个表达式的计算结果：" + expressionResult1);
                    System.out.println("第二个表达式的计算结果：" + expressionResult2);

                    //todo 5：根据告警类型的id进行判断触发告警
                    //todo 5.1：全部车辆告警
                    if (ruleInfoModel.getMonitorTypeId().equals("1")) {
                        ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult1 && expressionResult2);
                    }
                    //todo 5.2：车型告警
                    else if (ruleInfoModel.getMonitorTypeId().equals("2") &&
                            vehicleInfoModel.getModelCode().equals(ruleInfoModel.getModelCode())) {
                        ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult1 && expressionResult2);
                    }
                    //todo 5.3：区域告警
                    else if (ruleInfoModel.getMonitorTypeId().equals("3") &&
                            itcastDataPartObj.getProvince().equals(ruleInfoModel.getProvince()) &&
                            itcastDataPartObj.getCity().equals(ruleInfoModel.getCity())) {
                        ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult1 && expressionResult2);
                    }
                    //todo 5.4：大客户告警、车辆告警
                    else if (ruleInfoModel.getMonitorTypeId().equals("4") || ruleInfoModel.getMonitorTypeId().equals("5")) {
                        //拆分vin的数组对象
                        ArrayList<String> arrayVin = Lists.newArrayList(ruleInfoModel.getVins().split(","));
                        if (arrayVin.contains(itcastDataPartObj.getVin())) {
                            ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult1 && expressionResult2);
                        }
                    }
                    //todo 5.5：车系告警
                    else if (ruleInfoModel.getMonitorTypeId().equals("6") &&
                            vehicleInfoModel.getSeriesCode().equals(ruleInfoModel.getSeriesCode())) {
                        ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult1 && expressionResult2);
                    }

                } else {
                    //前两个字段必须同时存在（存在一个表达式的场景）
                    if(isExistsField(itcastDataPartObj, ruleInfoModel.getAlarmParam1Field()) &&
                            isExistsField(itcastDataPartObj, ruleInfoModel.getAlarmParam2Field())) {
                        //这是只有一个表达式的场景
                        //获取到第一个参数字段
                        //获取到第二个参数字段
                        boolean expressionResult = CustomRuleUtil.expression(
                                Double.parseDouble(BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam1Field())),
                                Double.parseDouble(BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam2Field())),
                                ruleInfoModel.getOperator1(),
                                ruleInfoModel.getAlarmThreshold1(),
                                ruleInfoModel.getRuleSymbol1(),
                                ruleInfoModel.getAlarmThreshold2(),
                                ruleInfoModel.getRuleSymbol2(),
                                ruleInfoModel.getLogicalSymbol()
                        );
                        System.out.println("第一个表达式的计算结果：" + expressionResult);

                        //todo 5：根据告警类型的id进行判断触发告警
                        //todo 5.1：全部车辆告警
                        if (ruleInfoModel.getMonitorTypeId().equals("1")) {
                            ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult);
                        }
                        //todo 5.2：车型告警
                        else if (ruleInfoModel.getMonitorTypeId().equals("2") &&
                                vehicleInfoModel.getModelCode().equals(ruleInfoModel.getModelCode())) {
                            ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult);
                        }
                        //todo 5.3：区域告警
                        else if (ruleInfoModel.getMonitorTypeId().equals("3") &&
                                itcastDataPartObj.getProvince().equals(ruleInfoModel.getProvince()) &&
                                itcastDataPartObj.getCity().equals(ruleInfoModel.getCity())) {
                            ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult);
                        }
                        //todo 5.4：大客户告警、车辆告警
                        else if (ruleInfoModel.getMonitorTypeId().equals("4") || ruleInfoModel.getMonitorTypeId().equals("5")) {
                            //拆分vin的数组对象
                            ArrayList<String> arrayVin = Lists.newArrayList(ruleInfoModel.getVins().split(","));
                            if (arrayVin.contains(itcastDataPartObj.getVin())) {
                                ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult);
                            }
                        }
                        //todo 5.5：车系告警
                        else if (ruleInfoModel.getMonitorTypeId().equals("6") &&
                                vehicleInfoModel.getSeriesCode().equals(ruleInfoModel.getSeriesCode())) {
                            ruleAlarmResultModel = convertCustomRuleAlarmResultModel(tuple2, ruleInfoModel, expressionResult);
                        }
                    }
                }

                //返回自定义告警规则分析结果对象不为空才追加到返回的列表对象
                if (Objects.nonNull(ruleAlarmResultModel)) {
                    //将计算好的告警规则分析后的结果追加到返回的集合列表中
                    resultModelArrayList.add(ruleAlarmResultModel);
                }
            }
        }

        //返回计算好的结果
        collector.collect(resultModelArrayList);
    }

    /**
     * 返回自定义告警规则分析结果对象
     * @param tuple2            原始窗口流数据中的某条记录
     * @param ruleInfoModel     当前针对于窗口流中的数据应用的自定义告警规则
     * @param expressionResult  当前数据是否触发了告警
     * @return
     */
    private CustomRuleAlarmResultModel convertCustomRuleAlarmResultModel(Tuple2<ItcastDataPartObj, VehicleInfoModel> tuple2,
                                                                         RuleInfoModel ruleInfoModel, boolean expressionResult){
        //告警标记: 1：告警 0：不告警
        int alarmFlag = 0;
        if(expressionResult){
            alarmFlag = 1;
        }

        //返回分析好的结果对象
        return new CustomRuleAlarmResultModel(
                tuple2.f0.getVin(),
                tuple2.f0.getTerminalTime(),
                alarmFlag,
                Integer.parseInt(ruleInfoModel.getAlarmFrame()),
                ruleInfoModel.getRuleName(),
                tuple2.f1.getSeriesName(),
                tuple2.f1.getSeriesCode(),
                tuple2.f1.getModelName(),
                tuple2.f1.getModelCode(),
                tuple2.f0.getProvince(),
                tuple2.f0.getCity(),
                tuple2.f0.getCountry(),
                tuple2.f0.getDistrict(),
                tuple2.f0.getAddress(),
                tuple2.f0.getLat(),
                tuple2.f0.getLng(),
                Integer.parseInt(ruleInfoModel.getId())
        );
    }
    /**
     * 查看字段是否在ItcastDataPartObj对象中是否存在这个属性
     * @param itcastDataPartObj
     * @return
     */
    private boolean isExistsFourFields(ItcastDataPartObj itcastDataPartObj, RuleInfoModel ruleInfoModel) {
        try {
            BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam1Field());
            BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam2Field());
            BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam3Field());
            BeanUtils.getProperty(itcastDataPartObj, ruleInfoModel.getAlarmParam4Field());
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * 根据指定字段名称去javaBean对象中回去属性
     * 不存在有两种场景：
     *    1：字段是空
     *    2：字段不为空，但是字段的名称是错误的（javaBean不存在该属性）
     * @param itcastDataPartObj
     * @param fieldName
     * @return
     */
    private boolean isExistsField(ItcastDataPartObj itcastDataPartObj, String fieldName){
        try {
            BeanUtils.getProperty(itcastDataPartObj, fieldName);
            Double.parseDouble(BeanUtils.getProperty(itcastDataPartObj, fieldName));
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * 查看字段是否在ItcastDataPartObj对象中是否存在这个属性
     * @param itcastDataPartObj
     * @return
     */
    private boolean isExistsFourFields(ItcastDataPartObj itcastDataPartObj, String fieldName) {
        try {
            BeanUtils.getProperty(itcastDataPartObj, fieldName);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    /**
     * 第一个自定义告警规则广播数据流的操作方法（自定义告警规则广播流）
     * @param ruleInfoModels
     * @param collector
     * @throws Exception
     */
    @Override
    public void flatMap2(ArrayList<RuleInfoModel> ruleInfoModels, Collector<ArrayList<CustomRuleAlarmResultModel>> collector) throws Exception {
        this.exprssionList = ruleInfoModels;
        System.out.println("获取到自定义告警规则的广播流数据>>>");
    }
}
