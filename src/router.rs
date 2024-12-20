use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Default)]
pub struct Router {
    forward_routes: HashMap<SocketAddr, SocketAddr>, // Maps input -> output
    backward_routes: HashMap<SocketAddr, SocketAddr>, // Maps output -> input
}

#[allow(dead_code)]
impl Router {
    pub fn new() -> Router {
        Router {
            forward_routes: HashMap::new(),
            backward_routes: HashMap::new(),
        }
    }

    pub fn add_route(&mut self, input: SocketAddr, output: SocketAddr) {
        self.forward_routes.insert(input, output);
        self.backward_routes.insert(output, input);
    }

    pub fn route(mut self, input: SocketAddr, output: SocketAddr) -> Self {
        self.add_route(input, output);
        self
    }

    pub fn add_direct_routes<PortRange: IntoIterator<Item = u16>>(
        &mut self,
        input_ip: IpAddr,
        output_ip: IpAddr,
        port_range: PortRange,
    ) {
        port_range.into_iter().for_each(|port: u16| {
            self.add_route(
                SocketAddr::new(input_ip, port),
                SocketAddr::new(output_ip, port),
            );
        });
    }

    pub fn direct_routes<PortRange: IntoIterator<Item = u16>>(
        mut self,
        input_ip: IpAddr,
        output_ip: IpAddr,
        port_range: PortRange,
    ) -> Self {
        self.add_direct_routes(input_ip, output_ip, port_range);
        self
    }

    pub fn add_offset_routes<PortRange: IntoIterator<Item = u16>>(
        &mut self,
        input_ip: IpAddr,
        input_port_range: PortRange,
        output_ip: IpAddr,
        output_port_range: PortRange,
    ) -> Option<()> {
        let mut input_ports = input_port_range.into_iter();
        let mut output_ports = output_port_range.into_iter();

        loop {
            match (input_ports.next(), output_ports.next()) {
                (Some(input_port), Some(output_port)) => {
                    self.add_route(
                        SocketAddr::new(input_ip, input_port),
                        SocketAddr::new(output_ip, output_port),
                    );
                }
                (None, None) => break, // Both ranges are fully iterated
                _ => return None,      // Mismatched range lengths
            }
        }

        Some(())
    }

    pub fn offset_routes<PortRange: IntoIterator<Item = u16>>(
        mut self,
        input_ip: IpAddr,
        input_port_range: PortRange,
        output_ip: IpAddr,
        output_port_range: PortRange,
    ) -> Option<Self> {
        self.add_offset_routes(input_ip, input_port_range, output_ip, output_port_range);
        Some(self)
    }

    pub fn required_ports(&self) -> HashSet<u16> {
        self.forward_routes.keys().map(|key| key.port()).collect()
    }

    pub fn required_sockets(routes: &HashMap<SocketAddr, SocketAddr>) -> HashSet<SocketAddr> {
        routes.keys().cloned().collect()
    }

    pub fn get_forward_routes(&self) -> &HashMap<SocketAddr, SocketAddr> {
        &self.forward_routes
    }

    pub fn get_backward_routes(&self) -> &HashMap<SocketAddr, SocketAddr> {
        &self.backward_routes
    }

    pub fn to_forward_backward_routes(
        self,
    ) -> (
        HashMap<SocketAddr, SocketAddr>,
        HashMap<SocketAddr, SocketAddr>,
    ) {
        (self.forward_routes, self.backward_routes)
    }
}
