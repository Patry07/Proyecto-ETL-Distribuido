// ETL epidemiológico en C++17. El mismo algoritmo admite ejecución secuencial,
// OpenMP y particiones por identificador para Docker Compose / Kubernetes.
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>
#include <omp.h>

namespace fs = std::filesystem;
using Clock = std::chrono::steady_clock;
struct Row { std::vector<std::string> cells; std::size_t order; };
struct Table { std::vector<std::string> header; std::vector<Row> rows; };
struct Stats {
    std::size_t dataset=0, input=0, exact=0, ids=0, invalid_age=0, output=0;
    int requested=1, actual=1, shard=0, shards=1;
    double read=0, global=0, kernel=0, write=0, total=0;
};
struct Options {
    fs::path input="datos/COVID19MEXICO.csv", output="resultados/COVID19MEXICO_limpio.csv";
    fs::path audit="resultados/auditoria.json", merge;
    std::string mode="seq";
    int threads=1, shard=0, shards=1;
};
double elapsed(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now()-start).count();
}
std::string trim(std::string_view s) {
    const auto a=s.find_first_not_of(" \t\r\n\v\f");
    if (a==std::string_view::npos) return {};
    const auto b=s.find_last_not_of(" \t\r\n\v\f");
    return std::string(s.substr(a,b-a+1));
}
bool is_na(const std::string& s) {
    static const std::unordered_set<std::string> names={
        "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan",
        "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null"};
    return names.count(s)>0;
}
bool number(std::string_view s, double& value) {
    if (s.empty()) return false;
    if (s.front()=='+') s.remove_prefix(1);
    auto r=std::from_chars(s.data(),s.data()+s.size(),value);
    return r.ec==std::errc{} && r.ptr==s.data()+s.size() && std::isfinite(value);
}
std::string canonical_number(double v) {
    char buf[96];
    auto r=std::to_chars(buf,buf+sizeof(buf),v,std::chars_format::general);
    if (r.ec!=std::errc{}) throw std::runtime_error("Número no representable");
    return std::string(buf,r.ptr);
}
// CSV con comillas escapadas, campos multilínea, CRLF y BOM UTF-8.
Table read_csv(const fs::path& path, bool infer_types=true) {
    std::ifstream f(path,std::ios::binary);
    if (!f) throw std::runtime_error("No se puede leer "+path.string());
    std::string raw((std::istreambuf_iterator<char>(f)),{}), field;
    std::vector<std::string> record;
    Table table;
    bool quoted=false, closed=false;
    auto finish_record=[&]() {
        record.push_back(std::move(field)); field.clear(); closed=false;
        if (record.size()==1 && record[0].empty()) { record.clear(); return; }
        if (table.header.empty()) table.header=std::move(record);
        else {
            if (record.size()!=table.header.size())
                throw std::runtime_error("CSV irregular: fila "+std::to_string(table.rows.size()+2));
            table.rows.push_back({std::move(record),table.rows.size()});
        }
        record.clear();
    };
    std::size_t pos=raw.compare(0,3,"\xef\xbb\xbf")==0 ? 3 : 0;
    for (;pos<raw.size();++pos) {
        char c=raw[pos];
        if (quoted) {
            if (c=='"') {
                if (pos+1<raw.size() && raw[pos+1]=='"') { field+='"'; ++pos; }
                else { quoted=false; closed=true; }
            } else field+=c;
        } else if (c=='"' && field.empty() && !closed) quoted=true;
        else if (c==',') { record.push_back(std::move(field)); field.clear(); closed=false; }
        else if (c=='\n' || c=='\r') {
            if (c=='\r' && pos+1<raw.size() && raw[pos+1]=='\n') ++pos;
            finish_record();
        } else {
            if (closed) throw std::runtime_error("Carácter después de comilla de cierre");
            field+=c;
        }
    }
    if (quoted) throw std::runtime_error("CSV con comillas sin cerrar");
    if (!field.empty() || !record.empty() || closed) finish_record();
    if (table.header.empty()) throw std::runtime_error("CSV sin encabezados");
    if (!infer_types) return table;
    // Equivale a la inferencia de columnas numéricas de read_csv en este esquema.
    // Se hace antes de eliminar duplicados, igual que en los notebooks originales.
    for (std::size_t c=0;c<table.header.size();++c) {
        bool numeric=true, any=false;
        for (auto& row:table.rows) {
            auto& s=row.cells[c];
            if (is_na(s)) { s.clear(); continue; }
            double value;
            if (!number(trim(s),value)) numeric=false;
            any=true;
        }
        if (numeric && any) for (auto& row:table.rows) {
            auto& s=row.cells[c];
            if (!s.empty()) { double v=0; number(trim(s),v); s=canonical_number(v); }
        }
    }
    return table;
}
int column(const Table& t,const std::string& name) {
    auto it=std::find(t.header.begin(),t.header.end(),name);
    return it==t.header.end() ? -1 : static_cast<int>(it-t.header.begin());
}
std::string row_key(const Row& row) {
    std::string key;
    for (const auto& s:row.cells) { key+=std::to_string(s.size()); key+=':'; key+=s; }
    return key;
}
std::uint64_t stable_hash(std::string_view s) {
    std::uint64_t h=14695981039346656037ULL;
    for (unsigned char c:s) { h^=c; h*=1099511628211ULL; }
    return h;
}
bool valid_iso_date(std::string_view s) {
    if (s.size()!=10 || s[4]!='-' || s[7]!='-') return false;
    for (std::size_t i=0;i<10;++i) if (i!=4 && i!=7 && (s[i]<'0' || s[i]>'9')) return false;
    const int y=std::stoi(std::string(s.substr(0,4)));
    const int m=std::stoi(std::string(s.substr(5,2)));
    const int d=std::stoi(std::string(s.substr(8,2)));
    if (y<1 || y>9999 || m<1 || m>12 || d<1) return false;
    const bool leap=(y%4==0 && y%100!=0) || y%400==0;
    const int days[]={31,28+static_cast<int>(leap),31,30,31,30,31,31,30,31,30,31};
    return d<=days[m-1];
}
void prepare_global(Table& t,Stats& stats,const Options& o) {
    for (auto& h:t.header) {
        h=trim(h);
        for (char& c:h) if (c>='a' && c<='z') c=static_cast<char>(c-'a'+'A');
    }
    if (std::set<std::string>(t.header.begin(),t.header.end()).size()!=t.header.size())
        throw std::runtime_error("Encabezados duplicados después de normalizar");
    const int id=column(t,"ID_REGISTRO");
    std::unordered_set<std::string> seen_rows, seen_ids;
    seen_rows.reserve(t.rows.size()); seen_ids.reserve(t.rows.size());
    std::vector<Row> kept; kept.reserve(t.rows.size());
    for (auto& row:t.rows) {
        const auto key=row_key(row);
        // ID sin recortar: la deduplicación ocurre antes de limpiar textos.
        const auto partition_key=id>=0 ? row.cells[id] : key;
        if (stable_hash(partition_key)%static_cast<unsigned>(o.shards)!=static_cast<unsigned>(o.shard)) continue;
        ++stats.input;
        if (!seen_rows.insert(key).second) { ++stats.exact; continue; }
        if (id>=0 && !seen_ids.insert(row.cells[id]).second) { ++stats.ids; continue; }
        kept.push_back(std::move(row));
    }
    t.rows=std::move(kept);
}
std::size_t transform_row(Row& row,const std::vector<int>& dates,int age) {
    for (auto& s:row.cells) s=trim(s);
    for (int c:dates) {
        auto& s=row.cells[c];
        if (s=="9999-99-99" || !valid_iso_date(s)) s.clear();
    }
    std::size_t invalid=0;
    if (age>=0) {
        auto& s=row.cells[age]; double v=0;
        if (!number(s,v)) s.clear();
        else if (v<0 || v>120) { s.clear(); invalid=1; }
        else s=canonical_number(v);
    }
    return invalid;
}
void transform(Table& t,Stats& stats,const Options& o) {
    std::vector<int> dates;
    for (const auto& name:{"FECHA_ACTUALIZACION","FECHA_INGRESO","FECHA_SINTOMAS","FECHA_DEF"}) {
        int c=column(t,name); if (c>=0) dates.push_back(c);
    }
    const int age=column(t,"EDAD");
    const auto count=static_cast<long long>(t.rows.size());
    std::size_t invalid=0;
    if (o.mode=="omp") {
        omp_set_dynamic(0);
        // Cada hilo modifica filas distintas; reducción para el contador común.
        #pragma omp parallel num_threads(o.threads) reduction(+:invalid) default(none) shared(t,stats,dates,age,count)
        {
            #pragma omp single
            stats.actual=omp_get_num_threads();
            #pragma omp for schedule(static)
            for (long long i=0;i<count;++i) invalid+=transform_row(t.rows[i],dates,age);
        }
    } else {
        for (auto& row:t.rows) invalid+=transform_row(row,dates,age);
    }
    stats.invalid_age=invalid;
    stats.output=t.rows.size();
}
void quote(std::ostream& out,const std::string& s) {
    if (s.find_first_of(",\"\r\n")==std::string::npos) { out<<s; return; }
    out<<'"';
    for (char c:s) { if (c=='"') out<<'"'; out<<c; }
    out<<'"';
}
void ensure_parent(const fs::path& p) {
    if (!p.parent_path().empty()) fs::create_directories(p.parent_path());
}
void write_csv(const Table& t,const fs::path& path,bool indices) {
    ensure_parent(path);
    std::ofstream out(path,std::ios::binary);
    if (!out) throw std::runtime_error("No se puede escribir "+path.string());
    out<<"\xef\xbb\xbf";
    auto line=[&](const std::vector<std::string>& cells) {
        for (std::size_t i=0;i<cells.size();++i) { if (i) out<<','; quote(out,cells[i]); }
        out<<'\n';
    };
    line(t.header);
    for (const auto& row:t.rows) line(row.cells);
    out.close();
    if (!out) throw std::runtime_error("Error al completar CSV");
    if (indices) {
        std::ofstream idx(path.string()+".indices");
        for (const auto& row:t.rows) idx<<row.order<<'\n';
        idx.close();
        if (!idx) throw std::runtime_error("Error al escribir índices");
    }
}
void write_audit(const Stats& s,const Options& o) {
    ensure_parent(o.audit);
    std::ofstream f(o.audit);
    if (!f) throw std::runtime_error("No se puede guardar auditoría");
    f<<std::setprecision(12)<<"{\n"
     <<"  \"implementacion\": \"C++17/"<<(o.merge.empty()?o.mode:"merge")<<"\",\n"
     <<"  \"filas_dataset\": "<<s.dataset<<",\n"
     <<"  \"filas_entrada\": "<<s.input<<",\n"
     <<"  \"duplicados_exactos_eliminados\": "<<s.exact<<",\n"
     <<"  \"duplicados_id_eliminados\": "<<s.ids<<",\n"
     <<"  \"edades_invalidas_convertidas_a_nulo\": "<<s.invalid_age<<",\n"
     <<"  \"filas_salida\": "<<s.output<<",\n"
     <<"  \"hilos_solicitados\": "<<s.requested<<",\n"
     <<"  \"hilos_utilizados\": "<<s.actual<<",\n"
     <<"  \"particion\": "<<s.shard<<",\n"
     <<"  \"particiones\": "<<s.shards<<",\n"
     <<"  \"lectura_segundos\": "<<s.read<<",\n"
     <<"  \"preparacion_global_segundos\": "<<s.global<<",\n"
     <<"  \"transformacion_filas_segundos\": "<<s.kernel<<",\n"
     <<"  \"etl_segundos\": "<<s.global+s.kernel<<",\n"
     <<"  \"escritura_segundos\": "<<s.write<<",\n"
     <<"  \"total_segundos\": "<<s.total<<",\n"
     <<"  \"openmp_version\": "<<_OPENMP<<"\n}\n";
    f.close(); if (!f) throw std::runtime_error("Error al completar auditoría");
}
void merge_parts(const Options& o) {
    const auto start=Clock::now();
    Table combined;
    std::unordered_set<std::size_t> indices;
    for (int p=0;p<o.shards;++p) {
        const auto path=o.merge/("parte_"+std::to_string(p)+".csv");
        Table part=read_csv(path,false);
        if (p==0) combined.header=part.header;
        if (part.header!=combined.header) throw std::runtime_error("Esquemas distintos entre particiones");
        std::ifstream f(path.string()+".indices");
        if (!f) throw std::runtime_error("Faltan índices de partición "+std::to_string(p));
        for (auto& row:part.rows) {
            if (!(f>>row.order) || !indices.insert(row.order).second)
                throw std::runtime_error("Índices faltantes o repetidos");
            combined.rows.push_back(std::move(row));
        }
        std::size_t extra; if (f>>extra) throw std::runtime_error("Sobran índices en una partición");
    }
    std::sort(combined.rows.begin(),combined.rows.end(),[](const Row& a,const Row& b){return a.order<b.order;});
    Stats s; s.input=s.output=s.dataset=combined.rows.size(); s.shards=o.shards;
    s.read=elapsed(start);
    const auto w=Clock::now(); write_csv(combined,o.output,false); s.write=elapsed(w);
    s.total=elapsed(start); write_audit(s,o);
    std::cout<<"Particiones reunidas: "<<o.shards<<"; filas: "<<s.output<<'\n';
}
int positive(const std::string& v,bool zero=false) {
    int n=0; auto r=std::from_chars(v.data(),v.data()+v.size(),n);
    if (r.ec!=std::errc{} || r.ptr!=v.data()+v.size() || n<(zero?0:1))
        throw std::runtime_error("Entero inválido: "+v);
    return n;
}
Options arguments(int argc,char** argv) {
    Options o;
    for (int i=1;i<argc;++i) {
        const std::string key=argv[i];
        if (key=="--help") {
            std::cout<<"etl --input CSV --output CSV --audit JSON --mode seq|omp --threads N\n"
                       "    [--shard-index I --shard-count N]\n"
                       "etl --merge-dir CARPETA --shard-count N --output CSV --audit JSON\n";
            std::exit(0);
        }
        if (++i>=argc) throw std::runtime_error("Falta valor para "+key);
        const std::string value=argv[i];
        if (key=="--input") o.input=value;
        else if (key=="--output") o.output=value;
        else if (key=="--audit") o.audit=value;
        else if (key=="--mode") o.mode=value;
        else if (key=="--threads") o.threads=positive(value);
        else if (key=="--shard-index") o.shard=positive(value,true);
        else if (key=="--shard-count") o.shards=positive(value);
        else if (key=="--merge-dir") o.merge=value;
        else throw std::runtime_error("Argumento desconocido: "+key);
    }
    if (o.mode!="seq" && o.mode!="omp") throw std::runtime_error("Modo inválido");
    if (o.shard>=o.shards) throw std::runtime_error("Índice de partición fuera de rango");
    if (o.output==o.input || o.audit==o.input || o.audit==o.output)
        throw std::runtime_error("Las rutas de entrada, salida y auditoría deben ser distintas");
    return o;
}
int main(int argc,char** argv) {
    try {
        // Utilidades mínimas para el Pod lector: la imagen final no necesita shell.
        if (argc==3 && std::string(argv[1])=="--hold") {
            std::this_thread::sleep_for(std::chrono::seconds(positive(argv[2]))); return 0;
        }
        if (argc==3 && std::string(argv[1])=="--read-file") {
            std::ifstream f(argv[2],std::ios::binary);
            if (!f) throw std::runtime_error("No se puede abrir archivo solicitado");
            std::cout<<f.rdbuf(); return std::cout?0:1;
        }
        const auto o=arguments(argc,argv);
        if (!o.merge.empty()) { merge_parts(o); return 0; }
        Stats s; s.requested=o.mode=="seq"?1:o.threads; s.shard=o.shard; s.shards=o.shards;
        const auto start=Clock::now();
        auto t=read_csv(o.input); s.dataset=t.rows.size(); s.read=elapsed(start);
        const auto g=Clock::now(); prepare_global(t,s,o); s.global=elapsed(g);
        const auto k=Clock::now(); transform(t,s,o); s.kernel=elapsed(k);
        const auto w=Clock::now(); write_csv(t,o.output,o.shards>1); s.write=elapsed(w);
        s.total=elapsed(start); write_audit(s,o);
        std::cout<<"C++ "<<o.mode<<" | hilos="<<s.actual<<" | partición="<<o.shard<<'/'<<o.shards
                 <<" | filas="<<s.output<<" | ETL="<<s.global+s.kernel<<" s | total="<<s.total<<" s\n";
        return 0;
    } catch (const std::exception& e) { std::cerr<<"ERROR: "<<e.what()<<'\n'; return 1; }
}
