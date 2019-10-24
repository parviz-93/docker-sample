import org.jetbrains.annotations.NotNull;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedisTestUtils {

    public static void clearRadisSet(@NotNull String connect, @NotNull String setName) {
        RedissonClient redissonClient = createClient(connect);
        redissonClient.getSet(setName).clear();
        redissonClient.shutdown();
    }

    private static RedissonClient createClient(@NotNull String connect) {
        String address = "redis://" + System.getProperty("redis", connect);
        Config config = new Config();
        config.useSingleServer().setAddress(address);
        return Redisson.create(config);
    }

}
