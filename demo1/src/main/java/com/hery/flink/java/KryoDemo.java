package com.hery.flink.java;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

/**
 * @Date 2020/7/8 13:02
 * @Created by hery
 * @Description 序列化
 */
public class KryoDemo {
    public static void main(String[] args) {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 注册类，如果传递给Flink算子的数据类型是父类，实际运行过程中使用的是子类，
        // 子类中有一些父类没有的数据结构和特性，将子类注册可以提高性能
        env.getConfig().registerKryoType(Dog.class);
        // 注册序列化器
        env.getConfig().registerTypeWithKryoSerializer(Dog.class,DogKryoSerializer.class);
    }
    public static abstract class Animal {
    }

    public static class Dog extends Animal {
        private final String name;

        public Dog(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
    public static class DogKryoSerializer extends Serializer<Dog> implements Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public void write(Kryo kryo, Output output, Dog object) {
            output.writeString(object.getName());
        }

        @Override
        public Dog read(Kryo kryo, Input input, Class<Dog> type) {
            return new Dog(input.readString());
        }
    }
}
