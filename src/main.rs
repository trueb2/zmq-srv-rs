
pub struct Client {
    pub socket: zmq::Socket,
}


pub struct Server {
    pub frontend: zmq::Socket,
    pub backend: zmq::Socket,
}

pub struct ServerWorker { }

impl Client {
    pub fn new(ctx: &zmq::Context) -> Result<Client, zmq::Error> {
        Ok(Client {
            socket: ctx.socket(zmq::DEALER)?,
        })
    }

    pub fn run() -> Result<(), Error> {
        println!("Starting client ...");
        
        let context = zmq::Context::new();
        let socket = context.socket(zmq::DEALER)?;

        let client_identity = uuid::Uuid::new_v4().to_string();
        socket.set_identity(client_identity.as_bytes())?;
        let addr = "tcp://localhost:5555";
        socket.connect(addr)?;

        println!("Connected client to {}", addr);

        let start = std::time::Instant::now();
        let mut request_id = 0;
        loop {
            if socket.poll(zmq::POLLIN, 1)? > 0 {
                let _msgs = socket.recv_multipart(0)?;
                /*
                let mut msg_str = String::new();
                for msg in msgs {
                    msg_str += std::str::from_utf8(&msg)?;
                }
                println!("Client {} Received '{}'", client_identity, msg_str);
                */
            }

            let request = format!("Client {} Request {}", client_identity, request_id);
            request_id += 1;
            socket.send(&request, 0)?;
            if request_id % 10000 == 0 {
                println!("{}\t|| '{}'", request_id as f32 / start.elapsed().as_secs_f32(), request);
            }
        }
    }
}

impl Server {
    pub fn new(ctx: &zmq::Context) -> Result<Server, zmq::Error> {
        Ok(Server {
            frontend: ctx.socket(zmq::ROUTER)?,
            backend: ctx.socket(zmq::DEALER)?,
        })
    }

    pub fn run() -> Result<(), Error> {
        println!("Starting server ...");

        let context = std::sync::Arc::new(zmq::Context::new());
        let server = Server::new(context.as_ref())?;

        server.frontend.bind("tcp://*:5555")?;
        server.backend.bind("inproc://backend")?;

        const MAX_WORKERS: i32 = 2;
        let mut worker_handles = vec![];
        for i in 0..MAX_WORKERS {
			let ctx = context.clone();
            worker_handles.push(std::thread::spawn(move || {
                ServerWorker::run(i, ctx.as_ref())
            }));
        }

        zmq::proxy(&server.frontend, &server.backend)?;
        
        for handle in worker_handles {
            let _ = handle.join()?;
        }

        println!("Stopping server ...");
        Ok(())
    }
}

impl ServerWorker {
    fn run(worker_id: i32, context: &zmq::Context) -> Result<(), Error> {
        let socket = context.socket(zmq::DEALER)?;
        socket.connect("inproc://backend")?;

        println!("Starting server worker {} ...", worker_id);

        loop {
            let identity = socket.recv_msg(0)?;
            let message = socket.recv_msg(0)?;

            // println!("Worker {}: Processed {:?} and {:?}", worker_id, std::str::from_utf8(&identity)?, std::str::from_utf8(&message)?);

            socket.send(identity, zmq::SNDMORE)?;
            socket.send(message, 0)?;
        }
    }
}

#[derive(Debug)]
pub struct Error {
    details: String,
}

impl Error {
    pub fn new(details: &str) -> Error {
        Error {
            details: String::from(details),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        self.details.as_ref()
    }
}

impl From<zmq::Error> for Error {
    fn from(err: zmq::Error) -> Self {
        Error::new(&format!("{}", err.to_string()))
    }
}

impl From<std::boxed::Box<dyn std::any::Any + std::marker::Send>> for Error {
    fn from(_: std::boxed::Box<dyn std::any::Any + std::marker::Send>) -> Self {
        Error::new(&format!("Failed with generic error"))
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::new(&format!("{}", err.to_string()))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = clap::App::new("ZMQ Actor")
        .version("1.0")
        .author("Jacob Trueb jwtrueb@gmail.com")
        .about("Configurable actor in ZMQ interactions")
        .arg(clap::Arg::with_name("socket_type")
                .short("s")
                .long("socket-type")
                .value_name("SOCKET_TYPE")
                .help("The type of socket that this Actor will use")
                .takes_value(true))
        .get_matches();

    let mut handles = vec![];
    handles.push(std::thread::spawn(|| { Server::run() }));
    for _ in 0..8 {
        handles.push(std::thread::spawn(|| { Client::run() }));
    }

    let mut ok = true;
    for handle in handles {
        match handle.join() {
            Ok(_) => {},
            Err(err) => { 
                println!("Thread join failed with {:?}", err); 
                ok = false; 
            }
        }
    }

    if ok {
        Ok(())
    } else {
        Err(Box::new(Error::new("Bad error code from join")))
    }
}

