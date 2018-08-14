package com.my.mba.local;

import java.text.DecimalFormat;
import java.util.*;

/**
 * Apriori算法（关联规则算法）
 */
public class Apriori {
    DecimalFormat df = new DecimalFormat("0.00");

    private double minSupport = 0.00; //最小支持度
    private double minConfidence = 0.00; //最小置信度
    private int maxPass = 2; //最大的k项
    //初始化交易数据
    private DataBase dataBase = new DataBase();
    //存放当前的候选项集的列表
    private List<ItemSet> candidateItemSets = new LinkedList<>();
    //存放当前的频繁项集的列表
    private List<ItemSet> frequentItemSets = new LinkedList<>();

    private void run() {
        System.out.println("开始进行计算");
        //第一阶段：生成频繁项集
        //1. 生成1-项频繁项集（即初始化C1以及计算L1）
        initializeFrequentItemSets();
        int pass = 1; //进行k + 1（即1+1）项的频繁项集的计算
        //计算出maxPass项的频繁项集
        while (candidateItemSets.size() > 0 && pass <= maxPass) {
            System.out.println("frequent Item list size: " + frequentItemSets.size());
            printItemSet("frequent Item set:", frequentItemSets);
            // 计算并构建（pass + 1）项候选项集列表
            generateCandidateItemSets(pass);
            // 打印候选集信息
            System.out.println("candidate size before prune: " + candidateItemSets.size());
            printItemSet("Candidate set:", candidateItemSets);
            //修剪频繁项集列表
            pruneCandidateItemSets();
            // 打印候选集信息
            System.out.println("candidate size after  prune: " + candidateItemSets.size());
            printItemSet("Candidate set:", candidateItemSets);
            // 将计算出来的候选项集列表拷贝给itemset列表
            generateFrequentItemSets();
            // 计算 pass+2 项候选集
            pass++;
        }

        System.out.println("*** Done ***");
        printItemSet("Item set:", frequentItemSets);
        System.out.println();
        System.out.println("*** Association rules");

        // 第二阶段，根据频繁项集生成关联规则
        List<AssociationRule> associationRules = generateAssociationRules(frequentItemSets);
        // 打印出最终的关联规则
        printFinalAssociationRules(associationRules);

    }

    /**
     * 1项频繁项集的计算
     */
    private void initializeFrequentItemSets() {
        candidateItemSets.clear();
        candidateItemSets.addAll(dataBase.getAllItems());//C1
        //对第一次的候选项集进行修剪
        pruneCandidateItemSets();
        // 将修剪之后的候选项集作为当前的频繁项集
        generateFrequentItemSets();
    }

    /**
     * 修剪候选集列表
     */
    private void pruneCandidateItemSets() {
        for (ItemSet itemSet : candidateItemSets) {
            //移除支持度不足minSupport的项集
            if (itemSet.getSupportCount() < dataBase.getTotalNumberOfTransactions() * minSupport)
                candidateItemSets.remove(itemSet);
        }
    }

    /**
     * 将修剪之后的候选项集作为当前的频繁项集
     */
    private void generateFrequentItemSets() {
        frequentItemSets.clear();
        frequentItemSets.addAll(candidateItemSets);
    }

    private void printItemSet(String title, List<ItemSet> sets) {
        System.out.println(title);
        for (ItemSet item : sets) {
            System.out.print("   {");
            for (String s : item.getItems())
                System.out.print(s + " ");
            System.out.print("}");
            System.out.print(" --- ");
            System.out.println(item.getSupportCount());
        }
    }

    /**
     * 生成pass+1项候选项集
     *
     * @param pass
     */
    private void generateCandidateItemSets(int pass) {
        candidateItemSets.clear();
        // 循环遍历所有的频繁项集
        for (int i = 0; i < frequentItemSets.size(); i++) {
            //从当前项集的下一项集开始遍历
            for (int j = i; j < frequentItemSets.size(); j++) {
                //拿到当前项集
                Set<String> s1 = frequentItemSets.get(i).getItems();
                //拿到下一项集
                Set<String> s2 = frequentItemSets.get(j).getItems();
                //看s1和s2能否组成pass+1项集
                boolean isCandiate = isCandiate(pass, s1, s2);
                if (isCandiate) {
                    // 如果s1和s2可以组合成pass-项候选集的话，则将两者联合起来构成一个pass+1-项候选集
                    Set<String> union = new HashSet<>(s1);
                    union.addAll(s2);//因为是Set，会消除重复元素

                    if (isSetAlreadyInCandidateList(union)) {
                        continue;
                    }

                    ItemSet newItem = new ItemSet(union);
                    int supportCount = dataBase.getNumberOfItemSet(newItem.getItems());
                    newItem.setSupportCount(supportCount);
                    //将pass+1项候选集放到候选集列表中
                    candidateItemSets.add(newItem);
                }
            }
        }
    }

    /**
     * 看看项集s1和项集s2能不能组合成候选pass+1-项集
     * 比如：
     * 项集s1为{A, B, D}
     * 项集s2为{A, B, C}
     * desiredItemSetSize = 3的话，
     * 则这个方法返回true，因为 {A, B, D} retainAll {A, B, C} = {A, B}.size() == 3 -1
     * 然后这两个项集就可以促成{A,B,C,D}项集
     *
     * @param pass 期望项集的项数
     * @param s1   pass 项集
     * @param s2   pass 项集
     * @return 如果项集s1和项集s2可以组合成pass+1项候选集的话则返回true，否则返回false
     */
    private boolean isCandiate(int pass, Set<String> s1, Set<String> s2) {
        Set<String> intersection = new HashSet<String>(s1);
        intersection.retainAll(s2); //取交集
        return intersection.size() == pass - 1;
    }

    /**
     * 是否该项集已经在候选列表中了
     *
     * @param s
     * @return
     */
    private boolean isSetAlreadyInCandidateList(Set<String> s) {
        for (ItemSet itemSet : candidateItemSets) {
            if (itemSet.equals(s))
                return true;

        }

        return false;
    }

    /**
     * 产生关联规则，规则为：
     * Antecedent --> Consequent [support, confidence]
     *
     * @param frequentItemSets
     * @return
     */
    private List<AssociationRule> generateAssociationRules(List<ItemSet> frequentItemSets) {
        List<AssociationRule> associationRules = new LinkedList<>();
        //循环遍历每一个项集
        for (ItemSet item : frequentItemSets) {
            System.out.print("Item set   {");
            for (String s : item.getItems())
                System.out.print(s + " ");
            System.out.println("}");
            // 计算当前频繁项集的support count
            int itemSetSupportCount = dataBase.getNumberOfItemSet(item.getItems());
            List<List<String>> powerSubItemSets = new LinkedList<List<String>>(); //存放当前项集的所有子集
            List<String> itemSet = new LinkedList<>(item.getItems());
            // 计算出当前项集中包含的所有的子集
            buildPowerSubItemSets(powerSubItemSets, itemSet);
            // 按照子集的长度小大进行升序排，排序后：
            //List(List(A), List(B), List(C), List(D)，
            //      List(A, B), List(A, C), List(A, D), List(B, C), List(B, D), List(C, D),
            //      List(A, B, C), List(A, B, D), List(A, C, D), List(B, C, D), List(A, B, C, D))
            Collections.sort(powerSubItemSets, new Comparator<List<String>>() {
                @Override
                public int compare(List<String> o1, List<String> o2) {
                    return o1.size() - o2.size();
                }
            });
            //删除最后一个元素，即删除List(A, B, C, D)
            powerSubItemSets.remove(powerSubItemSets.size() - 1);

            // 遍历所有的子集
            for (int i = 0; i < powerSubItemSets.size(); i++) {
                System.out.print("{");
                //拿到当前子集作为Antecedent
                List<String> antecedent = powerSubItemSets.get(i);
                // 计算Antecedent的support count
                int antecedentSupportCount = dataBase.getNumberOfItemSet(new HashSet<>(antecedent));
                for (String s : antecedent)
                    System.out.print(s + " ");
                System.out.print("} ==> {");
                // 当前项集减去Antecedent就是Consequent
                List<String> consequent = new LinkedList<String>(itemSet);
                consequent.removeAll(antecedent);
                for (String s : consequent) {
                    System.out.print(s + " ");
                }

                System.out.print("}  C:");
                System.out.print(itemSetSupportCount + "/" + antecedentSupportCount);
                // 计算支持度
                // sup((antecedent, consequent)) / 所有的交易数
                double support = (double) itemSetSupportCount / (double) dataBase.getTotalNumberOfTransactions();
                // 计算置信度
                // sup((antecedent, consequent)) / sup(antecedent)
                double confidence = (double) itemSetSupportCount / (double) antecedentSupportCount;
                System.out.print(" = " + df.format(confidence));

                // 过滤去置信度大于等于最小的置信度
                if (confidence >= minConfidence) {
                    // Sort to make display a little easier to read.
                    Collections.sort(antecedent);
                    Collections.sort(consequent);
                    associationRules.add(new AssociationRule(antecedent, consequent, confidence, support));
                }
            }

            System.out.println();

        }

        return associationRules;
    }

    private void printFinalAssociationRules(List<AssociationRule> associationRules) {
        System.out.println("*** Final association rules with minimum confidence percentage: " + minConfidence);

        for (AssociationRule ar : associationRules) {
            System.out.print("C:" + df.format(ar.getConfidence()));
            System.out.print("  S:");
            System.out.print(df.format(ar.getSupport()));
            System.out.print("  {");
            for (String s : ar.getAntecedent()) System.out.print(s + " ");
            System.out.print("} ==> {");
            for (String s : ar.getConsequent()) System.out.print(s + " ");
            System.out.println("}");
        }
    }

    /**
     * list => List(A, B, C, D)
     * powerSet => List(List(A, B, C, D), List(A, B, C), List(A, B, D), List(A, C, D), List(B, C, D),
     * List(A, B), List(A, C), List(A, D), List(B, C), List(B, D), List(C, D)
     * List(A), List(B), List(C), List(D))
     *
     * @param powerSet
     * @param list
     */
    private void buildPowerSubItemSets(List<List<String>> powerSet, List<String> list) {
        if (list.size() == 0)
            return;

        if (powerSet.contains(list))
            return;

        powerSet.add(list);

        for (int i = 0; i < list.size(); i++) {
            List<String> temp = new LinkedList<String>(list);
            temp.remove(i);
            buildPowerSubItemSets(powerSet, temp);
        }
    }

    public static void main(String[] args) {
        Apriori apriori = new Apriori();
        apriori.run();
    }
}
