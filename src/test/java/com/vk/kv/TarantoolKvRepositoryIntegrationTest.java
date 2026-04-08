import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers
class TarantoolKvRepositoryIntegrationTest {

    @Container
    private static final GenericContainer<?> TARANTOOL =
            new GenericContainer<>(DockerImageName.parse("tarantool/tarantool:latest"))
                    .withExposedPorts(3301)
                    .withEnv("TARANTOOL_USER", "app")
                    .withEnv("TARANTOOL_PASSWORD", "app_password")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("tarantool/init.lua"),
                            "/opt/tarantool/init.lua"
                    )
                    .withCommand("tarantool", "/opt/tarantool/init.lua")
                    .waitingFor(Wait.forLogMessage(".*Tarantool KV is ready.*\\n", 1));
}