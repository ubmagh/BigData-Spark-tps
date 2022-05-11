package getting_started.Application3;

import java.io.Serializable;



public class EmployeeBean implements Serializable {
    private long id;
    private String name;
    private String phone;
    private String departement;
    private long age;
    private double salary;

    public EmployeeBean() {
    }

    public EmployeeBean(long id, String name, String phone, String departement, int age, double salary) {
        this.id = id;
        this.name = name;
        this.phone = phone;
        this.departement = departement;
        this.age = age;
        this.salary = salary;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getDepartement() {
        return departement;
    }

    public void setDepartement(String departement) {
        this.departement = departement;
    }

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }
}
