#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Acked,
    Nacked,
}
