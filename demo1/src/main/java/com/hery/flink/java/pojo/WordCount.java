package com.hery.flink.java.pojo;

/**
 * @Date 2020/7/2 17:07
 * @Created by hery
 * @Description pojo
 * 1、类是公共的并且是独立的，没有非静态的内部类
 * 2、有公共无参构造函数
 * 3、类中的属性要么是公共的，要么提供get，set方法
 * 4、注意：当用户定义的数据类型不能被识别为pojo,必须将其处理为GenerictType并使用Kryo序列化
 */
public class WordCount {
    public String word;
    public int count;
    public WordCount() {}
    public WordCount(String word, int count) { this.word = word; this.count = count; }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
