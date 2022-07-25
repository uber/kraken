module github.com/uber/kraken

go 1.14

require (
	cloud.google.com/go/storage v1.6.0
	github.com/Microsoft/hcsshim v0.9.3 // indirect
	github.com/alecthomas/kingpin v2.2.6+incompatible
	github.com/alicebob/gopher-json v0.0.0-20180125190556-5a6b3ba71ee6 // indirect
	github.com/alicebob/miniredis v2.5.0+incompatible
	github.com/andres-erbsen/clock v0.0.0-20160526145045-9e14626cd129
	github.com/aws/aws-sdk-go v1.21.4
	github.com/awslabs/amazon-ecr-credential-helper v0.3.1
	github.com/bugsnag/bugsnag-go v1.5.0 // indirect
	github.com/bugsnag/panicwrap v0.0.0-20180510051541-1d162ee1264c // indirect
	github.com/c2h5oh/datasize v0.0.0-20171227191756-4eba002a5eae
	github.com/cactus/go-statsd-client v3.1.1+incompatible
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/containerd/cgroups v1.0.4 // indirect
	github.com/containerd/containerd v1.5.7
	github.com/containerd/continuity v0.0.0-00010101000000-000000000000 // indirect
	github.com/containerd/fifo v1.0.0 // indirect
	github.com/docker/distribution v2.7.1+incompatible
	github.com/docker/docker-credential-helpers v0.6.3
	github.com/docker/engine-api v0.0.0-20160908232104-4290f40c0566
	github.com/docker/go-events v0.0.0-20190806004212-e31b211e4f1c // indirect
	github.com/docker/go-metrics v0.0.0-20181218153428-b84716841b82 // indirect
	github.com/docker/libtrust v0.0.0-20160708172513-aabc10ec26b7 // indirect
	github.com/garyburd/redigo v1.6.0
	github.com/go-chi/chi v4.0.2+incompatible
	github.com/gofrs/uuid v0.0.0-20190320161447-2593f3d8aa45 // indirect
	github.com/gogo/googleapis v1.4.1 // indirect
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.0
	github.com/gomodule/redigo v2.0.0+incompatible // indirect
	github.com/gorilla/handlers v0.0.0-20190227193432-ac6d24f88de4 // indirect
	github.com/gorilla/mux v1.7.3
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/jackpal/bencode-go v0.0.0-20180813173944-227668e840fa
	github.com/jinzhu/gorm v1.9.16
	github.com/jmoiron/sqlx v0.0.0-20190319043955-cdf62fdf55f6
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0 // indirect
	github.com/mattn/go-sqlite3 v1.14.0
	github.com/opencontainers/go-digest v1.0.0
	github.com/pressly/goose v2.6.0+incompatible
	github.com/satori/go.uuid v1.2.0
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/stretchr/testify v1.7.0
	github.com/syndtr/gocapability v0.0.0-20200815063812-42c35b437635 // indirect
	github.com/uber-go/tally v3.3.11+incompatible
	github.com/willf/bitset v0.0.0-20190228212526-18bd95f470f9
	github.com/yuin/gopher-lua v0.0.0-20191128022950-c6266f4fe8d7 // indirect
	github.com/yvasiyarov/go-metrics v0.0.0-20150112132944-c25f46c4b940 // indirect
	github.com/yvasiyarov/gorelic v0.0.0-20180809112600-635ca6035f23 // indirect
	github.com/yvasiyarov/newrelic_platform_go v0.0.0-20160601141957-9c099fbc30e9 // indirect
	go.uber.org/atomic v1.5.0
	go.uber.org/multierr v1.4.0 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/net v0.0.0-20210825183410-e898025ed96a
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1
	google.golang.org/api v0.22.0
	gopkg.in/validator.v2 v2.0.0-20180514200540-135c24b11c19
	gopkg.in/yaml.v2 v2.3.0
)

replace github.com/docker/distribution => github.com/docker/distribution v0.0.0-20191024225408-dee21c0394b5

replace github.com/containerd/containerd => github.com/containerd/containerd v1.3.10

replace github.com/containerd/continuity => github.com/containerd/continuity v0.1.0

replace github.com/opencontainers/runc => github.com/opencontainers/runc v1.0.0-rc10
