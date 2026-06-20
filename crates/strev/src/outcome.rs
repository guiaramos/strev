#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Acked(sealed::AckToken),
    Nacked(sealed::AckToken),
}

impl Outcome {
    pub(crate) fn acked() -> Self {
        Self::Acked(sealed::AckToken(()))
    }

    pub(crate) fn nacked() -> Self {
        Self::Nacked(sealed::AckToken(()))
    }

    pub fn is_acked(self) -> bool {
        matches!(self, Self::Acked(_))
    }

    pub fn is_nacked(self) -> bool {
        matches!(self, Self::Nacked(_))
    }
}

mod sealed {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct AckToken(pub(super) ());
}
