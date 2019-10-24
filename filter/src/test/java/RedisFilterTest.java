import com.sbt.hackathon.filter.Filter;
import com.sbt.hackathon.filter.impl.redis.RedisFilterFactory;
import org.junit.*;

import java.util.Arrays;
import java.util.List;

public class RedisFilterTest {

    private static final String connect = "0.0.0.0:6379";
    private static final String setName = "TEST_SET";

    private static RedisFilterFactory filterFactory;
    private Filter filter;

    @BeforeClass
    public static void beforeClass() {
        filterFactory = new RedisFilterFactory(connect);
    }

    @AfterClass
    public static void afterClass() {
        filterFactory.close();
    }

    @Before
    public void before() {
        RedisTestUtils.clearRadisSet(connect, setName);
        filter = filterFactory.getInstance(setName);
    }

    @Test
    public void baseTest() {
        List<byte[]> result = filter.filtrate(Arrays.asList("first".getBytes(), "second".getBytes()));
        Assert.assertEquals(2, result.size());
    }

    @Test
    public void duplicateTest() {
        List<byte[]> result = filter.filtrate(Arrays.asList("first".getBytes(), "first".getBytes()));
        Assert.assertEquals(1, result.size());
    }

}
