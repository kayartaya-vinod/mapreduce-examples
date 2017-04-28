package com.viveknk;

public class Employee {

	String empid;
	String name;
	String dept;
	String sal;
	
	public Employee() {
		
	}
	
	public Employee(String details) {
		
		String[] allDetails = details.split(",");
		empid =  allDetails[0];
		name =  allDetails[1];
		dept =  allDetails[2];
		sal =  allDetails[3];
	}

	public String getEmpid() {
		return empid;
	}

	public void setEmpid(String empid) {
		this.empid = empid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDept() {
		return dept;
	}

	public void setDept(String dept) {
		this.dept = dept;
	}

	public String getSal() {
		return sal;
	}

	public void setSal(String sal) {
		this.sal = sal;
	}
	
	@Override
	public String toString() {
		return empid + "," + name + "," + dept + "," + sal;
	}
}