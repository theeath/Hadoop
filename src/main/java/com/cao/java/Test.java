package com.cao.java;

/**
 * Created by czf on 17-6-13.
 */
public class Test {
    public static void main(String[] args) {
        String s="3";
        System.out.println("3".equals(s));
        if ("1".equals(s)){
            System.out.println("1");
        }else if ("3".equals(s)){
            System.out.println("3");
        }else
            System.out.println("0");
    }
}
