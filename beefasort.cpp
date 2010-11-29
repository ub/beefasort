// beefasort version 0.2 26/10/2010

#include <fstream>
#include <iostream>
#include <iomanip>
#include <cstdint>
#include <cstdio>
#include <vector>
#include <list>
#include <algorithm>
#include <string>
#define BOOST_FILESYSTEM_VERSION 3
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <boost/scoped_array.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>

#define TBB_USE_CAPTURED_EXCEPTION 0

#include "tbb/parallel_sort.h"
#include "tbb/tick_count.h"
#include "tbb/tbb_exception.h"
#include "tbb/pipeline.h"
#include "tbb/parallel_invoke.h"



using tbb::tbb_exception;


using namespace std;
namespace bfs=boost::filesystem;
namespace po = boost::program_options;

typedef uint32_t THE_KEY_t;
const unsigned BYTES_IN_KEY=sizeof(THE_KEY_t);




const int DEFAULT_MAX_MEMUSE_IN_MB=256;
const size_t DEFAULT_MAX_MEMUSE=DEFAULT_MAX_MEMUSE_IN_MB * 1024 * 1024;
const int REASONABLE_MEMORY_LIMIT_IN_MB=4095;
struct my_options {
     string input_file;
     string output_file;
     string tmpdir;
     bool  keep_temporary;
     size_t max_memory_use;
     bool force_output_overwrite;
     bool print_timings;
     bool zero_length_input; // empty input file - a degenerate case

     my_options(): input_file(),
          output_file(),
          tmpdir(),
          keep_temporary(false),
          max_memory_use(DEFAULT_MAX_MEMUSE),
          force_output_overwrite(false),
          print_timings(false),
          zero_length_input(false) {}
};


size_t validate_memory_reqs(unsigned int req, unsigned int default_req, int depth=0)
{
     try {
          char * test=new char[req];
          delete[] test;
     } catch(std::bad_alloc& e) {
          if (depth > 3) {
               return 0;
          } else {
               unsigned int req1= req > default_req ? default_req : req >> 1;
               return validate_memory_reqs(req1,default_req, depth+1);
          }
     }
     return req;
}

#include "program_options.cpp.inc"

/************************ Timings reporting support **************************/

class timekeeper {
public:
     typedef tbb::tick_count::interval_t time_interval;
     typedef map<string,time_interval> dictionary;

     timekeeper(ostream * print_on_exit=0): output_in_desctructor(print_on_exit), tally(),order() {}
     ~timekeeper() {
          if(output_in_desctructor) print(*output_in_desctructor);
     }

     void add(const char * category, const time_interval& val) {
          string key(category);
          dictionary::iterator p=tally.find(key);
          if(p==tally.end()) {
               tally[key]=val;
               order.push_back(key);
          } else (*p).second += val;
     }

     void print(ostream& out) {
          out << "Времена выполнения фаз алгоритма:\n"  ;
          BOOST_FOREACH(const string& key, order) {

               out << key << ": " << (tally[key].seconds()) <<"\n";

          };
          out << endl;

     }

private:
     ostream * output_in_desctructor;
     dictionary tally;
     vector<string> order;
};

class stopwatch {
public:
     stopwatch(timekeeper* a, const char * category): acc(a),
          category_(category),
          start(tbb::tick_count::now()) {}
     ~stopwatch() {
          stop=tbb::tick_count::now();
          acc->add(category_,stop-start);

     }
private:
     timekeeper * acc;
     const char * category_;
     tbb::tick_count start, stop;
};





/******************* merger class *****************/
const size_t MIX_AND_RESORT_THRESHOLD= 124 * 1024;

namespace multi_merge {
class output_buffer;
class buffer {
public:
     typedef vector<THE_KEY_t> container_t;
     typedef container_t::iterator iterator;
     typedef container_t::const_iterator const_iterator;
     typedef pair<buffer::iterator,buffer::iterator> range;
     static const size_t RESERVE_DATA_PAST_END=0; // space reserved for sentinels.
     // set to 1024 for GNU multimerge test

     buffer(ifstream* ifs, const string& filename, size_t size_in_bytes, bool keep_file_when_done ):
          keep_file(keep_file_when_done),
          container(size_in_bytes/ BYTES_IN_KEY),
          hard_begin(container.begin()),hard_end(container.end() - RESERVE_DATA_PAST_END),
          current_(hard_begin),cut(hard_begin),end_(hard_begin), // before first read cut is at the begining
          input(ifs),input_filename(filename) {

     }
     ~buffer() {
          dispose_resources();
     }

     // buffer usable size (w/o space reserved for sentinels if any)
     // in elements (NOT bytes)
     size_t data_size() const {
          return hard_end - hard_begin;
     }

     bool read_from_stream() {
          size_t  in_buffer_size=data_size() * BYTES_IN_KEY;
          char * const input_buffer= reinterpret_cast<char * const>(& container.front());
          input->read(input_buffer,in_buffer_size);
          streamsize n=input->gcount();
          if(n==0) {

               end_=current_=hard_begin;
               return false;
          }
          current_=hard_begin;
          end_=current_+ (n / BYTES_IN_KEY);

          return true;
     }

     THE_KEY_t back() const {
          assert(end_!=current_);
          return * (end_ - 1);
     }

     iterator current() const {
          return current_;
     }
     void set_cut_pos(THE_KEY_t cut_value) {
          // set cut position with binary_search
          cut=std::upper_bound(current_,end_, cut_value);
     }

     // precondition: set_cut_pos has  already been called on current iteration
     // should  this buffer be used in the current in-memory merge step?
     bool is_mergeable() const {
          return cut > current_;
     }
     // same precondition
     size_t mergeable_count() const {
          return cut - current_;
     }
     range mergeable_range() const {
          return make_pair(current_,cut);
     }

     void mark_merge_done() {
          current_=cut;
     }

     // precondition: set_cut_pos has  already been called on current iteration
     // should be this buffer filled from the file stream at the end of current
     // iteration?
     bool recyclable() const {
          return current_==end_;
     }


     void dispose_resources() {
          input->close();
          if(! keep_file) bfs::remove(input_filename);

     }

//   friend void output_buffer::copy_from_source(buffer *);
     friend class output_buffer;

private:
     //non copyable
     buffer(const buffer&);
     buffer& operator=(const buffer&);
private:
     bool keep_file;
     container_t container;
     const iterator hard_begin,
     hard_end;
     iterator       current_,
     cut,
     end_;


     ifstream * input;
     string input_filename;

};

typedef buffer::range range;
typedef  std::list<range>  sources;

// in-memory parallelizable merge helpers
namespace inram {


// first value of next range is striclty less than last value of the current range
// precondition: both ranges not empty
inline bool ranges_overlap(const range& current, const range& next)
{
     return *next.first < *(current.second - 1);
}
// precondition: list must have at least one element
// list should have no empty ranges!
bool have_strict_overappling(const sources& list_of_ranges)
{
     sources::const_iterator i_first = list_of_ranges.begin(),
                                       end   = list_of_ranges.end();
     assert(i_first != end); // precondition
     sources::const_iterator i_last = --end;
     for(sources::const_iterator i=i_first; i!=i_last; i++) {
          sources::const_iterator i_next=i;
          i_next++;
          if(ranges_overlap(*i, *i_next)) return true;
          /*     THE_KEY_t current_last_val= *((*i).second -1),
                         next_first_val = *(*i_next).first;
                if(next_first_val < current_last_val) return true;*/
     }
     return false;
}

// precondition: no empty range allowed!
void sort(sources& list_of_ranges)
{
     list_of_ranges.sort( [](const range& a, const range& b)->bool {
          return *a.first < *b.first;
     }
                        );

}

// original becomes the lower partitions collection
// upper partitions  colection is filled
// After the partition function is invoked, we
// MUST NOT use original lists anymore
// Then it should be safe to recurse even with parallel invocation
void partition(sources&  original, sources& upper, THE_KEY_t pivot)
{


     for(sources::iterator i=original.begin(); i!=original.end(); ) {
          sources::iterator next_i=i;
          next_i++;
          buffer::iterator range_split_point=std::lower_bound((*i).first,(*i).second,pivot);

          if( range_split_point==(*i).second)
               // split equals to the range end.
               // the whole range goes to lower partition collection
          {
               /* Do nothing! Deliberately. Everything remains as it is*/
          } else if( range_split_point==(*i).first)
               // split equals to the range begin
               // the whole range goes to the upper partition collection
          {
               upper.splice(upper.end(),original,i); // remove from the original and append to
               // upper

          } else
               // split somewhere in the middle.
               // splitting the range in two
          {
               range upper_copy;
               upper_copy.first= range_split_point;
               upper_copy.second = (*i).second;
               (*i).second=range_split_point;
               upper.push_back( upper_copy);

          }

          i=next_i;
     }
     sort(upper);
}


size_t total_size(const sources& list_of_ranges)
{


     return std::accumulate(list_of_ranges.begin(), list_of_ranges.end(),0,
     [](size_t sum, const range& x)->size_t {
          return x.second - x.first + sum;
     }
                           );
}

template <typename Iterator>
void cat(const sources& inputs, Iterator output)
{
     BOOST_FOREACH(const range& each,inputs) {
          output=std::copy(each.first,each.second,output);
     }
}

template <typename Iterator>
void mix_and_resort(const sources& inputs, Iterator output_begin, Iterator output_end)
{

     cat(inputs,output_begin);
     std::sort(output_begin,output_end);
}

// precondition: the list should be sorted by the first value of each range
//               it must have at least two elements
//               and no empty ranges

THE_KEY_t find_pivot(const sources& list_of_ranges)
{
     sources::const_iterator max_upper=
          std::max_element(list_of_ranges.begin(),
                           list_of_ranges.end(),
                           [](const range& a,
     const range& b) -> bool {
          buffer::const_iterator a_last_it = a.second -1,
          b_last_it = b.second -1;
          return (*a_last_it) < (*b_last_it);
     }
                          );
     THE_KEY_t max_value = *((*max_upper).second - 1),
                           min_value = *(*list_of_ranges.begin()).first;

     // caclulate the rounded up mean value. I mean it.
     return  (min_value>>1) + (max_value>>1) + (1&(min_value|max_value));
}

// precondition: inputs not empty
template <typename RandomAccessIterator>
void merge( sources& inputs, RandomAccessIterator output, size_t combined_size)
{
     if(!have_strict_overappling(inputs)) {
          cat(inputs,output);
          return;
     }
     // Oher clever heuristics could be tried before mix_and_resort
     // WRITE HERE
     //
     if(combined_size < MIX_AND_RESORT_THRESHOLD) {
          mix_and_resort(inputs,output,output+combined_size);
          return;
     }
     // inputs with the size of one never have strict overlapping
     // we have at least 2-sized list here, if precondition is not violated.
     if(++(++inputs.begin()) == inputs.end()) { // list has size of 2

          std::merge(inputs.front().first,inputs.front().second,
                     inputs.back().first,inputs.back().second,
                     output
                    );
          return;
     }



     THE_KEY_t pivot= find_pivot(inputs);
     sources& lower=inputs; // alias - the old good 'inputs' variable prepares to lose identity for good
     sources upper;
     partition(inputs, upper, pivot); // NEVER ever use inputs again!
     //  It's gone. lower has taken it's place.
     size_t count_l= total_size(lower);
     size_t count_u= combined_size - count_l;

     RandomAccessIterator lower_out=output, upper_out=output+count_l;
     tbb::parallel_invoke(
          [&] {merge(lower, lower_out,count_l);},
          [&] {merge(upper, upper_out,count_u);});
}

};//namespace inram


class  output_buffer {
public:
     output_buffer( const string& filename, size_t size_in_bytes):
          container(size_in_bytes/BYTES_IN_KEY), out() {
          out.exceptions(ios_base::failbit | ios_base::badbit);
          out.open(filename, ios::binary);
     }

     THE_KEY_t * begin() {
          return &container.front();
     }

     void write(size_t elements_count) {

          char * const buf= reinterpret_cast<char * const>(& container.front());
          out.write(buf, elements_count * BYTES_IN_KEY);

     }
     // buffer usable size  in elements (NOT bytes)
     size_t data_size() {
          return container.size();     // no reserved space needed for output buffer
     }
     void copy_from_source(buffer * source) {
          char * const buf= reinterpret_cast<char * const>(& container.front());
          while(source->input->good()) {
               source->input->read(buf, data_size() * BYTES_IN_KEY);
               streamsize actual=source->input->gcount();
               out.write(buf, actual);
          }
     }


private:
     vector<THE_KEY_t> container;
     ofstream out;

};

class runs_manager {
public:
     typedef vector<shared_ptr<ifstream> > ifstream_container;
     typedef list<shared_ptr<buffer> > input_buffers_container;

     static size_t input_buffer_size( size_t total_memory_quota, int number_of_buffers) {
          size_t estimate=(total_memory_quota/number_of_buffers)>> 1;
          return estimate & ~static_cast<size_t>(0x0fff);
     }
     static size_t output_buffer_size( size_t total_memory_quota) {
          return total_memory_quota >> 1;
     }

     runs_manager(const vector<string>& run_names, const string& ofilename, size_t memory_quota, bool keep_temporary=false):
          ifstreams(),buffers(),obuffer(ofilename, output_buffer_size(memory_quota)) {
          int runs_count=run_names.size();
          ifstreams.reserve(runs_count);
          size_t buffer_size=input_buffer_size(memory_quota,runs_count);
          BOOST_FOREACH(  string filename, run_names) {
               shared_ptr<ifstream> shp_ifstream(new ifstream);
               shp_ifstream->exceptions(ios_base::failbit | ios_base::badbit); // throw exception if open fails
               shp_ifstream->open(filename, ios_base::binary);
               shp_ifstream->exceptions(ios_base::goodbit); // do not throw exceptions during input operation
               ifstreams.push_back(shp_ifstream);

               shared_ptr<buffer> shp_buffer(new buffer(shp_ifstream.get(),filename,buffer_size,keep_temporary));
               buffers.push_back(shp_buffer);
          }

     }

     void read_all_ready_buffers_and_remove_garbage() {
          for(input_buffers_container::iterator i=buffers.begin(); i!=buffers.end(); ) {
               input_buffers_container::iterator next_i=i;
               next_i++;
               if((*i)->recyclable()) {
                    bool exhausted= ! (*i)->read_from_stream();
                    if(exhausted)   buffers.erase(i); // No data read - the file has eofed!
                    // Destroy all associated resources and remove from list
               }
               i=next_i;
          }
     }

/// precondition: buffers not empty
     THE_KEY_t find_cut_value() const {
          assert(!buffers.empty());
          input_buffers_container::const_iterator imin =
               min_element(buffers.begin(),buffers.end(),
                           [](const shared_ptr<buffer>& pa,const shared_ptr<buffer>& pb) ->bool
          { return pa->back() < pb->back(); } );
          return (*imin)->back();
     }

     void set_all_cut_positions(THE_KEY_t cut_value) {
          BOOST_FOREACH( shared_ptr<buffer> p, buffers) {
               p->set_cut_pos(cut_value);
          }
     }
     // fill the list with the ranges sorted by first element of each range
     void select_mergeable_ranges( sources& destination) const {
          BOOST_FOREACH( shared_ptr<buffer> p, buffers) {
               if(p->is_mergeable()) destination.push_back(p->mergeable_range());
          }
          inram::sort(destination); // sort by first value of each range
     }

     void adjust_after_merge() {
          BOOST_FOREACH( shared_ptr<buffer> p, buffers) {
               if(p->is_mergeable()) p-> mark_merge_done();
          }
     }


     void do_merge_runs(timekeeper &tk) {
          while(buffers.size()> 1) {
               {
                    stopwatch tmp_timer(&tk,"read tmp files");
                    read_all_ready_buffers_and_remove_garbage();
               }
               if(buffers.empty()) break;
               THE_KEY_t cut=find_cut_value ();
               set_all_cut_positions(cut);
               sources merge_parts;
               select_mergeable_ranges(merge_parts);

               size_t output_size=inram::total_size(merge_parts);
               {

                    stopwatch tmp_timer(&tk,"merge in memory");

                    inram::merge(merge_parts, obuffer.begin(), output_size);
               }
               adjust_after_merge();
               stopwatch tmp_timer(&tk,"write output file");
               obuffer.write(output_size);
          }
          if(buffers.size()==1) {
               stopwatch tmp_timer(&tk,"write output file");
               obuffer.copy_from_source(buffers.front().get());
               buffers.erase(buffers.begin());
          }

     }

private:
     ifstream_container ifstreams;
     input_buffers_container buffers;
     output_buffer obuffer;
};
}; // namespace multi_merge




/************************ Temporary files management *************************/

class run_filenames_collection {
public:
     run_filenames_collection(const string& tmpdirname) :
          tmpdir(tmpdirname),
          unique_part(bfs::unique_path().string()),
          dir_file_path(tmpdir),
          rfn_format("BEEF_%04d_%s"),
          current_file_num(0) {
          dir_file_path /= (boost::format("BEEF_temp_%s") % unique_part).str();

     }

     string add() {

          return path_of_file(current_file_num++).string();
     }

     void save_dir() const {
          ofstream f;
          f.exceptions(ios_base::failbit | ios_base::badbit);
          f.open(dir_file_path.string());
          f << *this;
     }

     void get_run_file_names(vector<string>* names) const {
          for(int i=0; i<current_file_num; i++) {
               names->push_back(path_of_file(i).string());
          }
     }


     friend
     basic_ostream<char>& operator<<(basic_ostream<char>& os,const run_filenames_collection& x);



private:
     const bfs::path tmpdir;
     string unique_part;
     bfs::path dir_file_path;
     const boost::format rfn_format;
     int current_file_num;

     bfs::path path_of_file(int num) const {
          bfs::path p(tmpdir);
          return p /= (boost::format(rfn_format) %  num % unique_part).str();
     }

};

basic_ostream<char>& operator<<(basic_ostream<char>& os,const run_filenames_collection& rfnc)
{
     for(int i=0; i<rfnc.current_file_num; i++) {
          os << rfnc.path_of_file(i).string() <<"\n";
     }

     return  os;
}


class double_buffer {
public:
     typedef vector<THE_KEY_t> buffer_container_t;
     typedef buffer_container_t::iterator iterator;
     struct pipeline_token {

          pipeline_token():current_half(-1),begin(),n_elems(0) {}
          pipeline_token(const pipeline_token& src):
               current_half(src.current_half), begin(src.begin),n_elems(src.n_elems) {}

          pipeline_token(double_buffer& dbuf, int used_half, size_t n) :
               current_half(used_half),
               begin(dbuf.halves[current_half]),
               n_elems(n) {}


          const int current_half;
          const iterator begin;
          size_t n_elems;
     };
     friend class pipeline_token;
     explicit double_buffer(size_t total_buffers_size_in_bytes):
          elements_per_buffer(total_buffers_size_in_bytes/BYTES_IN_KEY/2),
          buffers_container(elements_per_buffer * 2) {
          halves[0]=buffers_container.begin();
          halves[1]=halves[0] + elements_per_buffer;
     }



     size_t read(ifstream& f, int current_half) {

          f.read(reinterpret_cast<char * const>(&*halves[current_half])
                 ,elements_per_buffer * BYTES_IN_KEY);
          return f.gcount()/BYTES_IN_KEY;
     }

     void write(ofstream& o, int current_half, size_t elements_count) {
          o.write(reinterpret_cast<char * const>(&*halves[current_half])
                  ,elements_count * BYTES_IN_KEY);
     }
private:  // prohibit copying
     double_buffer(const double_buffer&);
     double_buffer& operator=(const double_buffer&);

private:
     const size_t elements_per_buffer;
     buffer_container_t buffers_container;
     iterator halves[2];

};

/****************** Algorithm stages and helper functions ********************/

void write_output_file(double_buffer * pdbuf, int current_half,
                       size_t elements_count, const string& filename)
{
     ofstream d;
     d.exceptions(ios_base::failbit | ios_base::badbit); // throw exception on write error (e.g. disk full)
     d.open(filename, ios_base::binary);
     if(elements_count) pdbuf->write(d,current_half,elements_count);
}


//
//   create_sorted_runs() - stage 1 of our sort algorithm
//
//   return false if input file has been sorted and written
//   without any temporary files
bool create_sorted_runs(size_t memory_use,
                        const string& input_file_name,
                        const string& output_file_name,
                        run_filenames_collection& tmpfilenames,
                        bool keep_temporary)
{
     size_t io_buffer_size=memory_use;

     ifstream f;
     f.open(input_file_name, ios_base::binary);

     bool first_iteration=true; // is it the first time the loop being executed ?
     bool file_fits_in_single_buffer=false;
     double_buffer dbuf(io_buffer_size);
     int circular_index=0;

     // should use tbb::filter::serial_in_order in pipeline!
     auto input_lambda=[&] (tbb::flow_control&  fc) -> double_buffer::pipeline_token {
          int current_half = 1 & circular_index++;
          size_t n_elems=dbuf.read( f,  current_half);
          if(n_elems==0) {
               fc.stop();
               return double_buffer::pipeline_token();
          }
          if(first_iteration && f.peek()==EOF) {
               file_fits_in_single_buffer=true;
          }
          first_iteration=false;
          return double_buffer::pipeline_token(dbuf,current_half,n_elems);
     };
     auto sort_lambda=[](double_buffer::pipeline_token part) -> double_buffer::pipeline_token {
          tbb::parallel_sort(part.begin, part.begin+part.n_elems);
          return part;
     };
     // should use tbb::filter::serial_in_order in pipeline processing !
     auto output_lambda=[&](double_buffer::pipeline_token part) ->void {
          if(file_fits_in_single_buffer) {
               f.close(); // output file could be the same as input;
               // closing the input to be sure everything goes ok
               write_output_file(&dbuf,part.current_half, part.n_elems,output_file_name);
          } else{

               ofstream s;
               s.exceptions(ios_base::failbit | ios_base::badbit); // hopefully we'll catch "disk full"
               s.open(
                    tmpfilenames.add(), // creating another run file
                    ios_base::binary );

               dbuf.write(s, part.current_half, part.n_elems);
          }
     };
     typedef double_buffer::pipeline_token input_token;
     typedef double_buffer::pipeline_token output_token;

     tbb::parallel_pipeline(2, // only two parallel tokens are allowed for our double_buffer
                            tbb::make_filter<void,input_token>(tbb::filter::serial_in_order,input_lambda) &
                            tbb::make_filter<input_token,output_token>(tbb::filter::parallel,sort_lambda) &
                            tbb::make_filter<output_token,void>(tbb::filter::serial_in_order,output_lambda));

        return ! file_fits_in_single_buffer;
}





int main(int argc, char * argv[])
{
     my_options options=read_and_validate_program_options(argc,argv);

     timekeeper tk(options.print_timings ? &cerr : 0);
     stopwatch total_time(&tk,"TOTAL");

     run_filenames_collection tmpfilenames(options.tmpdir);


     try {
          if(options.zero_length_input) {
               // write zero length output file
               stopwatch tmp_timer(&tk,"write zero-length output");
               write_output_file(0,0,0, options.output_file);
               return 0;
          }

          bool tempfiles_created=false;
          {


               stopwatch tmp_timer(&tk,"STAGE 1");
               tempfiles_created=create_sorted_runs(options.max_memory_use,
                                                    options.input_file,
                                                    options.output_file,
                                                    tmpfilenames,
                                                    options.keep_temporary);
          }
          if(!tempfiles_created) return 0;
          if(options.keep_temporary) tmpfilenames.save_dir();
          vector<string> run_names;

          tmpfilenames.get_run_file_names(& run_names);


          //merge temporary files
          stopwatch tmp_timer(&tk,"merge tmp files");

          multi_merge::runs_manager engine(run_names, options.output_file, options.max_memory_use, options.keep_temporary);
          engine.do_merge_runs(tk);

     } catch(bfs::filesystem_error& e) {
          cerr<<"Ошибка работы с файлами\n\n";
          cerr<<e.what() <<endl;
          return 3;
     } catch(ios_base::failure& e) {
          cerr << "Ошибка ввода-вывода: ";
          cerr <<e.what() <<"\n";
          cerr << "Не хватает места на диске либо системный сбой"<<endl;
          return 4;

     } catch(std::bad_alloc) {
          cerr<<"Ошибка: не удалось удовлетворить запрос на выделение динамической памяти!\n" <<endl;
          return 4;
     } catch(tbb::captured_exception& e) {
          cerr<<"Ошибка работы с ресурсами.\n"
              "  Библиотека TBB перехватила исключение в фоновом потоке исполнения.\n\n"
              "  Дополнительная информация: " << e.what() <<endl;
          return 5;
     } catch(tbb_exception& e) {
          cerr<<"Ошибка: исключение в библиотеке TBB \n\n";
          cerr<<e.what() <<endl;
          return 6;
     }
     return 0;
}
