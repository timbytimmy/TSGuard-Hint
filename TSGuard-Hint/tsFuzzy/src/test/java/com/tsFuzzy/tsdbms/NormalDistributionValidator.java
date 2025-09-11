package com.tsFuzzy.tsdbms;

import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class NormalDistributionValidator {

    public static void main(String[] args) {
        int count = 30; // 需要选取的数字数量
        int min = 1; // 数字范围的最小值
        int max = 100; // 数字范围的最大值
        double mean = 50; // 正态分布的均值
        double stdDev = 16.67; // 正态分布的标准差

        List<Integer> selectedNumbers = getUniqueNormalDistributedNumbers(count, min, max, mean, stdDev);
        selectedNumbers.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
        System.out.println("选取的数字: " + selectedNumbers);
    }

    public static List<Integer> getUniqueNormalDistributedNumbers(int count, int min, int max, double mean, double stdDev) {
        Random random = new Random();
        Set<Integer> uniqueNumbers = new HashSet<>();

        while (uniqueNumbers.size() < count) {
            // 生成一个符合正态分布的随机数
            double value = mean + stdDev * random.nextGaussian();

            // 将其限制在[min, max]范围内并取整
            int intValue = (int) Math.round(value);
            if (intValue >= min && intValue <= max) {
                uniqueNumbers.add(intValue); // 保证数字不重复
            }
        }
        return new ArrayList<>(uniqueNumbers);
    }

}
