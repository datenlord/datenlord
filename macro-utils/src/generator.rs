//! Some types and the underlying implementations for the crate.

use core::mem;

use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::parse::Parse;
use syn::punctuated::Punctuated;
use syn::{parse_macro_input, LitByteStr, LitInt, Token};

/// Define custom keywords with [`syn::custom_keyword`].
macro_rules! custom_keyword {
    ($($kw:ident),* $(,)?) => {
        $(syn::custom_keyword!($kw);)*
    };
}

/// Custom keywords.
mod kw {
    custom_keyword!(u8, u16, u32, u64, i8, i16, i32, i64, str, BE, LE,);
}

/// The endian tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Endian {
    /// Big endian, token: `BE`
    Big,
    /// Little endian, token: `LE`
    Little,
}

impl Parse for Endian {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let lookahead = input.lookahead1();

        if lookahead.peek(kw::BE) {
            input.parse::<kw::BE>()?;
            Ok(Self::Big)
        } else if lookahead.peek(kw::LE) {
            input.parse::<kw::LE>()?;
            Ok(Self::Little)
        } else {
            Err(lookahead.error())
        }
    }
}

/// The items in [`generate_bytes`](fn@super::generate_bytes).
#[derive(Clone, Debug, PartialEq, Eq)]
enum Item {
    /// A `u8` item
    U8(u8),
    /// A `i8` item
    I8(i8),
    /// A `u16` item
    U16(u16),
    /// A `i16` item
    I16(i16),
    /// A `u32` item
    U32(u32),
    /// A `i32` item
    I32(i32),
    /// A `u64` item
    U64(u64),
    /// A `i64` item
    I64(i64),
    /// A `str` item
    Bytes(Vec<u8>),
}

impl Item {
    /// Convert an item into raw bytes in the specified `endian`.
    fn into_bytes(self, endian: Endian) -> Vec<u8> {
        match endian {
            Endian::Big => match self {
                Item::U8(val) => val.to_be_bytes().into(),
                Item::I8(val) => val.to_be_bytes().into(),
                Item::U16(val) => val.to_be_bytes().into(),
                Item::I16(val) => val.to_be_bytes().into(),
                Item::U32(val) => val.to_be_bytes().into(),
                Item::I32(val) => val.to_be_bytes().into(),
                Item::U64(val) => val.to_be_bytes().into(),
                Item::I64(val) => val.to_be_bytes().into(),
                Item::Bytes(val) => val,
            },
            Endian::Little => match self {
                Item::U8(val) => val.to_le_bytes().into(),
                Item::I8(val) => val.to_le_bytes().into(),
                Item::U16(val) => val.to_le_bytes().into(),
                Item::I16(val) => val.to_le_bytes().into(),
                Item::U32(val) => val.to_le_bytes().into(),
                Item::I32(val) => val.to_le_bytes().into(),
                Item::U64(val) => val.to_le_bytes().into(),
                Item::I64(val) => val.to_le_bytes().into(),
                Item::Bytes(val) => val,
            },
        }
    }

    /// Get the size of this item (in bytes).
    fn size(&self) -> usize {
        match *self {
            Item::U8(_) | Item::I8(_) => mem::size_of::<u8>(),
            Item::U16(_) | Item::I16(_) => mem::size_of::<u16>(),
            Item::U32(_) | Item::I32(_) => mem::size_of::<u32>(),
            Item::U64(_) | Item::I64(_) => mem::size_of::<u64>(),
            Item::Bytes(ref bytes) => bytes.len(),
        }
    }
}

/// A helper macro to build if-else arms in parse function of `Item`.
macro_rules! item_parse_arms {
    (
        $input:ident,
        $token_name:ident,
        ($kw:ident, $token:ident) => $blk:block,
        $(($kws:ident, $tokens:ident) => $blks:block),*
        $(,)?
    ) => {
        let lookahead = $input.lookahead1();

        if lookahead.peek(kw::$kw) {
            $input.parse::<kw::$kw>()?;
            $input.parse::<Token![:]>()?;
            let $token_name = $input.parse::<$token>()?;
            $blk
        }
        $(
            else if lookahead.peek(kw::$kws) {
                $input.parse::<kw::$kws>()?;
                $input.parse::<Token![:]>()?;
                let $token_name = $input.parse::<$tokens>()?;
                $blks
            }
        )*
        else {
            Err(lookahead.error())
        }
    };
}

impl Parse for Item {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        item_parse_arms! {
            input,
            token,
            (u8, LitInt) => {
                Ok(Self::U8(token.base10_parse()?))
            },
            (i8, LitInt) => {
                Ok(Self::I8(token.base10_parse()?))
            },
            (u16, LitInt) => {
                Ok(Self::U16(token.base10_parse()?))
            },
            (i16, LitInt) => {
                Ok(Self::I16(token.base10_parse()?))
            },
            (u32, LitInt) => {
                Ok(Self::U32(token.base10_parse()?))
            },
            (i32, LitInt) => {
                Ok(Self::I32(token.base10_parse()?))
            },
            (u64, LitInt) => {
                Ok(Self::U64(token.base10_parse()?))
            },
            (i64, LitInt) => {
                Ok(Self::I64(token.base10_parse()?))
            },
            (str, LitByteStr) => {
                Ok(Self::Bytes(token.value()))
            }
        }
    }
}

/// The parsed `generate_bytes!` content.
#[derive(Debug, Clone)]
struct Generator {
    /// The endian.
    endian: Endian,
    /// The comma punctuated items.
    items: Punctuated<Item, Token![,]>,
}

impl Generator {
    /// Get the sum of sizes of items
    fn size(&self) -> usize {
        self.items.iter().map(Item::size).sum()
    }
}

impl Parse for Generator {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let endian = input.parse()?;
        input.parse::<Token![;]>()?;
        let items = input.parse_terminated(Item::parse, Token![,])?;
        Ok(Generator { endian, items })
    }
}

impl ToTokens for Generator {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let Generator { endian, items } = self.clone();

        let bytes = items.into_iter().flat_map(|item| item.into_bytes(endian));
        let expanded = quote! {
            [#(#bytes),*]
        };
        tokens.extend(expanded);
    }

    fn to_token_stream(&self) -> TokenStream {
        let mut tokens = TokenStream::new();
        self.to_tokens(&mut tokens);
        tokens
    }

    fn into_token_stream(self) -> TokenStream
    where
        Self: Sized,
    {
        self.to_token_stream()
    }
}

/// The implementation of `generate_bytes!`.
pub fn generate_bytes_impl(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let generator = parse_macro_input!(input as Generator);
    generator.into_token_stream().into()
}

/// The implementation of `calculate_size!`.
pub fn calculate_size_impl(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let generator = parse_macro_input!(input as Generator);
    let size = generator.size();
    let expanded = quote!(#size);
    expanded.into()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use quote::quote;
    use syn::parse_quote;

    use super::{Endian, Generator, Item};

    #[test]
    fn test_endian_parse() {
        let be: Endian = parse_quote! {
            BE
        };
        assert_eq!(be, Endian::Big);

        let le: Endian = parse_quote! {
            LE
        };
        assert_eq!(le, Endian::Little);
    }

    #[test]
    fn test_endian_parse_fail() {
        let err = syn::parse2::<Endian>(quote!(NE)).unwrap_err();
        assert_eq!(err.to_string(), "expected `BE` or `LE`");
    }

    #[test]
    fn test_item_parse() {
        let item_u8: Item = parse_quote! {
            u8: 32
        };
        assert_eq!(item_u8, Item::U8(32));
        assert_eq!(item_u8.size(), 1);
        assert_eq!(item_u8.into_bytes(Endian::Big), vec![32]);

        let item_i64: Item = parse_quote! {
            i64: 0x20
        };
        assert_eq!(item_i64, Item::I64(32));
        assert_eq!(item_i64.size(), 8);
        assert_eq!(
            item_i64.clone().into_bytes(Endian::Big),
            vec![0, 0, 0, 0, 0, 0, 0, 32]
        );
        assert_eq!(
            item_i64.into_bytes(Endian::Little),
            vec![32, 0, 0, 0, 0, 0, 0, 0]
        );

        let item_str: Item = parse_quote! {
            str: b"hello, world\0"
        };
        assert_eq!(item_str, Item::Bytes(b"hello, world\0".to_vec()));
        assert_eq!(item_str.size(), 13);
        assert_eq!(
            item_str.into_bytes(Endian::Big),
            vec![0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00]
        );
    }

    #[test]
    fn test_item_parse_fail() {
        let err = syn::parse2::<Item>(quote!(u8: 1024)).unwrap_err();
        assert_eq!(err.to_string(), "number too large to fit in target type");

        let err = syn::parse2::<Item>(quote!(usize: 1024)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "expected one of: `u8`, `i8`, `u16`, `i16`, `u32`, `i32`, `u64`, `i64`, `str`"
        );

        let err = syn::parse2::<Item>(quote!(u8: b"str")).unwrap_err();
        assert_eq!(err.to_string(), "expected integer literal");

        let err = syn::parse2::<Item>(quote!(str: 1024)).unwrap_err();
        assert_eq!(err.to_string(), "expected byte string literal");
    }

    #[test]
    fn test_generator() {
        let generator: Generator = parse_quote! {
            BE;
            u8: 0,
            u8: 10,
            u16: 20,
            u32: 30
        };

        assert_eq!(generator.endian, Endian::Big);
        assert_eq!(generator.items.len(), 4);
        assert_eq!(generator.size(), 8);
    }
}
