package com.sidd.batch.job;

import lombok.*;

/**
 * @author siddarth.ajay
 */

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
public class Customer {

    private @NonNull Integer id;

    private String firstName;

    private String lastName;

    private String gender;

    private String dob;

    private String status;

    private Integer empNumber;


}