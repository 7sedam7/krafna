use std::fmt::{Debug, Display};

#[derive(Debug)]
pub struct PeekableDeque<T> {
    deque: Vec<T>,
    index: usize,
}

impl<T: Display> PeekableDeque<T> {
    // Method to peek at the next item without removing it
    pub fn peek(&self) -> Option<&T> {
        self.deque.get(self.index)
    }

    pub fn back(&mut self, n: usize) {
        self.index = self.index.saturating_sub(n)
    }

    pub fn end(&self) -> bool {
        self.index >= self.deque.len()
    }
}

impl<T: Display> Display for PeekableDeque<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str: String = self
            .deque
            .iter()
            .enumerate()
            .map(|(i, c)| {
                if i == self.index {
                    format!("[{}]", c)
                } else {
                    format!("{}", c)
                }
            })
            .collect();

        if self.end() {
            return write!(f, "{}[]", str);
        }

        write!(f, "{}", str)
    }
}

impl<T> FromIterator<T> for PeekableDeque<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        PeekableDeque {
            deque: iter.into_iter().collect(),
            index: 0,
        }
    }
}

impl<T: Clone> Iterator for PeekableDeque<T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.index += 1;
        self.deque.get(self.index).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_back_negative() {
        let query = "test".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        peekable_query.next();
        peekable_query.next();
        peekable_query.back(5);

        assert_eq!('t', *peekable_query.peek().unwrap());
    }

    #[test]
    fn test_back_positive() {
        let query = "test".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        peekable_query.next();
        peekable_query.next();
        peekable_query.back(1);

        assert_eq!('e', *peekable_query.peek().unwrap());
    }

    #[test]
    fn test_peek() {
        let query = "test".to_string();
        let peekable_query = PeekableDeque::from_iter(query.chars());

        if let Some(peeked_char) = peekable_query.peek() {
            if *peeked_char != 't' {
                panic!("Expected 't' char, but got {}", peeked_char);
            }
        } else {
            panic!("Expected 't' char, but got nothing");
        }
    }

    #[test]
    fn test_peek_when_empty() {
        let query = "".to_string();
        let peekable_query = PeekableDeque::from_iter(query.chars());

        if let Some(peeked_char) = peekable_query.peek() {
            panic!("Expected nothing, but got {}", peeked_char);
        }
    }

    #[test]
    fn test_next() {
        let query = "test".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        assert_eq!('e', peekable_query.next().unwrap());
        assert_eq!('e', *peekable_query.peek().unwrap());
    }

    #[test]
    fn test_next_when_empty() {
        let query = "".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        peekable_query.next();

        if let Some(peeked_char) = peekable_query.peek() {
            panic!("Expected nothing, but got {}", peeked_char);
        }
    }

    #[test]
    fn test_to_string() {
        let query = "test".to_string();
        let mut peekable_query = PeekableDeque::from_iter(query.chars());

        assert_eq!("[t]est", peekable_query.to_string());

        peekable_query.next();
        assert_eq!("t[e]st", peekable_query.to_string());

        peekable_query.next();
        assert_eq!("te[s]t", peekable_query.to_string());

        peekable_query.next();
        assert_eq!("tes[t]", peekable_query.to_string());
    }
}
