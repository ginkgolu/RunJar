package com.geostar.geosmarter.recommendation.entity;

import java.io.Serializable;

public class MyRelationShip implements Comparable<MyRelationShip>, Serializable {

    private static final long serialVersionUID = 1L;

    private String selfName;
    private String otherName;
    private int weight;

    public MyRelationShip(String selfName, String otherName, int weight) {
        super();
        this.selfName = selfName;
        this.otherName = otherName;
        this.weight = weight;
    }

    @Override
    public int compareTo(MyRelationShip o) {
        if (this.selfName.hashCode() < o.selfName.hashCode()) {
            if (this.otherName.hashCode() < o.otherName.hashCode()) {
                return this.weight - o.weight;
            }
            return this.otherName.hashCode() - o.otherName.hashCode();
        }
        return this.selfName.hashCode() - o.selfName.hashCode();
    }

    public String getSelfName() {
        return selfName;
    }

    public void setSelfName(String selfName) {
        this.selfName = selfName;
    }

    public String getOtherName() {
        return otherName;
    }

    public void setOtherName(String otherName) {
        this.otherName = otherName;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

}
