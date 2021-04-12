package cn.lizzy.learn.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName : TxEvent
 * @AUTHOR :  Yang XianWei
 * @DATE :    2021/4/12 19:52
 * @DESCRIPTION :
 * @since JDK 1.8
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {

    private String txId;
    private String payChannel;
    private Long eventTime;
}
