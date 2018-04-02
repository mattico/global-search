extern crate actix;
extern crate actix_web;
extern crate elasticlunr;
extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate futures;
#[macro_use]
extern crate log;
extern crate mdbook;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate tantivy;
extern crate tempdir;

use std::sync::Arc;

use actix::prelude::*;
use actix_web::{fs, middleware, Application, AsyncResponder, HttpRequest, HttpResponse,
                HttpServer, Method};
use elasticlunr::document_store::DocumentStore;
use futures::{Future, IntoFuture};
use mdbook::MDBook;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tantivy::collector::TopCollector;
use tantivy::query::{Query, QueryParser};
use tantivy::schema::*;
use tempdir::TempDir;

fn run() -> Result<()> {
    env_logger::Builder::from_env("SEARCH_LOG")
        .filter(Some("global_search"), log::LevelFilter::Info)
        .init();

    let mut schema_builder = SchemaBuilder::default();
    let book = schema_builder.add_text_field("book", STRING | STORED);
    let section = schema_builder.add_text_field("section", STRING | STORED);
    let title = schema_builder.add_text_field("title", TEXT | STORED);
    let breadcrumbs = schema_builder.add_text_field("breadcrumbs", TEXT | STORED);
    let body = schema_builder.add_text_field("body", TEXT | STORED);
    let schema = schema_builder.build();
    let tmp_dir = TempDir::new("bookshelf_index")?;
    let search_index = tantivy::Index::create(tmp_dir.path(), schema.clone())?;
    let mut index_writer = search_index.writer(100_000_000)?;

    let root = Path::new("..");
    let books = [
        "book/first-edition",
        "book/second-edition",
        "nomicon",
        "rust-by-example",
    ];
    for path in &books {
        {
            let book = MDBook::load(&root.join(path))
                .map_err(|e| e.chain_err(|| format!("Error Building Book {}", path)))?;
            book.build()?;
        }
        info!("Built {}", path);

        let index_path = root.join(path).join("book").join("searchindex.js");
        info!("Loading document store from {:?}", index_path);
        let mut index = String::new();
        File::open(index_path)?.read_to_string(&mut index)?;
        let index = &index[16..index.len() - 1];
        let index: serde_json::Value = serde_json::from_str(index)?;
        let docstore = index["index"]["documentStore"].clone();
        let docstore: DocumentStore = serde_json::from_value(docstore)?;

        if !docstore.save {
            warn!("Document Store saving disabled for book: {}", path);
            continue;
        }

        for (doc_ref, doc_fields) in &docstore.docs {
            index_writer.add_document(doc!(
                book => path.to_string(),
                section => doc_ref.clone(),
                title => doc_fields["title"].clone(),
                breadcrumbs => doc_fields["breadcrumbs"].clone(),
                body => doc_fields["body"].clone(),
            ));
        }
        info!("Added {:?} to Tantivy index", path);
    }

    index_writer.commit()?;
    search_index.load_searchers()?;
    info!("Search index ready");

    let sys = actix::System::new("global-search");

    // Should be less than NUM_SEARCHERS, currently 12
    let search_index = Arc::new(search_index);
    let addr = SyncArbiter::start(8, move || QueryExecutor {
        index: search_index.clone(),
        query_parser: QueryParser::for_index(&search_index, vec![title, breadcrumbs, body]),
        collector: TopCollector::with_limit(10),
    });

    let _server = HttpServer::new(move || {
        let mut app = Application::with_state(AppState {
            searcher: addr.clone(),
        }).middleware(middleware::Logger::default())
            .resource("/search", |r| r.method(Method::GET).f(query));
        for path in &books {
            let mut url = String::from("/bookshelf/");
            url.push_str(path);
            app = app.handler(
                &url,
                fs::StaticFiles::new(root.join(path).join("book"), true).index_file("index.html"),
            )
        }
        app
    }).bind("127.0.0.1:8080")?
        .start();

    info!("Listening on 127.1:8080");
    sys.run();

    Ok(())
}

struct AppState {
    searcher: Addr<Syn, QueryExecutor>,
}

fn query(req: HttpRequest<AppState>) -> Box<Future<Item = HttpResponse, Error = actix_web::Error>> {
    if let Some(query) = req.query().get("query").map(ToString::to_string) {
        req.state()
            .searcher
            .send(SearchQuery { query })
            .from_err()
            .and_then(|res| match res {
                Ok(resp) => Ok(HttpResponse::Ok()
                    .content_type("application/json")
                    .body(resp)
                    .into()),
                Err(_) => Ok(HttpResponse::InternalServerError()
                    .reason("Error executing search query")
                    .finish()
                    .into()),
            })
            .responder()
    } else {
        Box::new(
            Ok(HttpResponse::InternalServerError()
                .reason("Unable to find query URL parameter")
                .finish()
                .into())
                .into_future(),
        )
    }
}

struct SearchQuery {
    pub query: String,
}

impl Message for SearchQuery {
    type Result = Result<String>;
}

struct QueryExecutor {
    pub index: Arc<tantivy::Index>,
    pub query_parser: QueryParser,
    pub collector: TopCollector,
}

impl Actor for QueryExecutor {
    type Context = SyncContext<Self>;
}

impl Handler<SearchQuery> for QueryExecutor {
    type Result = Result<String>;

    fn handle(&mut self, msg: SearchQuery, _ctx: &mut Self::Context) -> Result<String> {
        let query: Box<Query> = self.query_parser
            .parse_query(&msg.query)
            .map_err(::tantivy::Error::from)?;
        let searcher = self.index.searcher();
        let schema = self.index.schema();
        searcher.search(&*query, &mut self.collector)?;

        // Our top collector now contains the 10
        // most relevant doc ids...
        let mut response_body = String::from("[");
        let doc_addresses = self.collector.docs();
        for doc_address in doc_addresses {
            let retrieved_doc = searcher.doc(&doc_address)?;
            response_body.push_str(&schema.to_json(&retrieved_doc));
            response_body.push(',');
            trace!("Address: {:?}", doc_address);
            trace!("Result: {}\n", schema.to_json(&retrieved_doc));
        }
        response_body.pop();
        response_body.push_str("]\n");

        Ok(response_body)
    }
}

quick_main!(run);

mod errors {
    error_chain! {
        links {
            MDBook(::mdbook::errors::Error, ::mdbook::errors::ErrorKind);
        }

        foreign_links {
            Io(::std::io::Error);
            SerdeJson(::serde_json::Error);
            Tantivy(::tantivy::Error);
        }
    }
}
use errors::*;
