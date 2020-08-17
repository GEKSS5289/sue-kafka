package com.sue.kafka.api.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author sue
 * @date 2020/8/17 11:44
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {
    private String id;
    private String name;
}
