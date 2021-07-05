package com.founder.bigdata.compute.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description:学生实体类
 * </p>
 *
 * @author Guan 2021/07/02 13:43
 * @program demo
 */
@Component
@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Student {
    private Integer id;
    private String name;
    private Integer age;
}
