package com.sym.canal;

import java.util.Scanner;

/**
 * @author shenyanming
 * @date 2020/7/26 14:20.
 */

public class CanalTest {
    public static void main(String[] args) {
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
