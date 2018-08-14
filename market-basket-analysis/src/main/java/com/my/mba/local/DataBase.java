package com.my.mba.local;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 原始数据类
 */
public class DataBase {
    //所有的交易数据
    private List<Set<String>> transactions = new ArrayList<Set<String>>();
    //所有item以及对应的支持数的集合
    private Map<String, Integer> allItems = new HashMap<>();

    public DataBase() {
        try {
            BufferedReader b = new BufferedReader(new InputStreamReader(new FileInputStream("F:\\BigData\\workspace\\spark-demo\\market-basket-analysis\\src\\main\\resources\\test\\test.csv")));
            String line = null;
            while ((line = b.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] items = line.split(",");
                Set<String> set = new HashSet<>();
                for (String item : items) {
                    set.add(item);
                    Integer support = allItems.get(item);
                    if (support != null) {
                        support += 1;
                        allItems.put(item, support);
                    } else {
                        allItems.put(item, 1);
                    }
                }

                transactions.add(set);
            }
        } catch (FileNotFoundException e) {
            System.out.println("test.csv文件没有找到！");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取所有交易数据的总记录数
    public int getTotalNumberOfTransactions() {
        return allItems.size();
    }

    /**
     * 获取该DataBase中所有交易包含的所有的item以及它的support_count
     *
     * @return
     */
    public List<ItemSet> getAllItems() {
        return allItems.entrySet().stream().map(entry -> {
            ItemSet itemSet = new ItemSet(new HashSet<String>() {{
                add(entry.getKey());
            }});
            itemSet.setSupportCount(entry.getValue());
            return itemSet;
        }).collect(Collectors.toList());
    }

    /**
     * 计算同时出现在项集items中所有item的交易的数量
     *
     * @return
     */
    public int getNumberOfItemSet(Set<String> itemSet) {
        int itemSetSupportCount = 0;
        // For each transaction.
        //循环遍历每条交易
        for (Set<String> transaction : transactions) {
            // See if this transaction contains all attributes.
            // 判断这条交易是否包含了项集itemSet中的每一条item
            boolean containsAllItems = true;
            for (String item : itemSet) {
                if (!transaction.contains(item)) {
                    containsAllItems = false;
                    break;
                }
            }
            // If the transaction contained all attributes, inc record count.
            // 如果包含了，则累加1
            if (containsAllItems) itemSetSupportCount++;
        }

        return itemSetSupportCount;
    }
}
