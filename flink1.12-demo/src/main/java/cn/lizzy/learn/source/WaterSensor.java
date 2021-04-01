package cn.lizzy.learn.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName : WaterSensor
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/3/31 19:50
 * @DESCRIPTION :
 * @since JDK 1.8
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {

    private String id;
    private Long ts;
    private Integer vc;
}
