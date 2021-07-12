package com.sym.canal;

import com.sym.canal.CanalClient;
import org.junit.Test;

import java.util.Scanner;

/**
 * @author shenyanming
 * Create on 2021/07/12 19:07
 */
public class CanalTest {

    @Test
    public void start() {
        CanalClient canalClient = new CanalClient();
        Scanner sc = new Scanner(System.in);

        while (true) {
            String s = sc.nextLine();
            if (!"q".equals(s)) {
                canalClient.start();
            } else {
                canalClient.stop();
                return;
            }
        }
    }
}
