package cn.abelib.kafka.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * @Author: abel.huang
 * @Date: 2019-09-18 23:42
 *  todo 还没有做线程安全
 */
@Slf4j
public class ApplicationExecutor {
    private static Executor executor;
    private static final int CORE_SIZE = 4;
    public static Executor getInstance() {
        executor = new ThreadPoolExecutor(CORE_SIZE,
                CORE_SIZE,
                0,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(CORE_SIZE),
                new ThreadFactoryBuilder()
                        .build(), (r, executor) -> {
            try {
                // 核心改造点，由LinkedBlockingDeque的offer改成put阻塞方法
                executor.getQueue().put(r);
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        });
        return executor;
    }

    private ApplicationExecutor() {

    }
}
