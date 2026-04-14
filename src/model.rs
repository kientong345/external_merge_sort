#[derive(Debug, Clone)]
pub struct ElementChunk {
    pub elements: Vec<u64>,
    pub read_cursor: usize,
}

impl ElementChunk {
    pub fn new(elements: Vec<u64>) -> Self {
        Self {
            elements,
            read_cursor: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.elements.len() - self.read_cursor
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn push_front(&mut self, value: u64) {
        if self.read_cursor > 0 {
            self.read_cursor -= 1;
            self.elements[self.read_cursor] = value;
        } else {
            self.elements.insert(0, value);
        }
    }

    pub fn pop_front(&mut self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            let val = self.elements[self.read_cursor];
            self.read_cursor += 1;
            Some(val)
        }
    }

    pub fn push_back(&mut self, value: u64) {
        self.elements.push(value);
    }

    pub fn pop_back(&mut self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            self.elements.pop()
        }
    }

    pub fn sort(&mut self) {
        self.elements.sort_unstable();
    }
}
