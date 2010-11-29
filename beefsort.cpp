// beefsort version 0.1 18/10/2010

#include <fstream>
#include <iostream>
#include <iomanip>
#include <cstdint>
#include <cstdio>
#include <vector>
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

using tbb::tbb_exception;


using namespace std;
namespace bfs=boost::filesystem;
namespace po = boost::program_options;

typedef uint32_t THE_KEY_t;
const unsigned BYTES_IN_KEY=sizeof(THE_KEY_t);




const int DEFAULT_MAX_MEMUSE_IN_MB=256;
const size_t DEFAULT_MAX_MEMUSE=DEFAULT_MAX_MEMUSE_IN_MB * 1024 * 1024;
const int REASONABLE_MEMORY_LIMIT_IN_MB=4095;
struct my_options
{
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
  try
  {
      char * test=new char[req];
      delete[] test;
  }
  catch(std::bad_alloc& e)
  {
     if (depth > 3) {return 0;}
     else {
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

timekeeper(ostream * print_on_exit=0): output_in_desctructor(print_on_exit), tally(),order(){}
~timekeeper(){ if(output_in_desctructor) print(*output_in_desctructor);}

void add(const char * category, const time_interval& val) {
    string key(category);
    dictionary::iterator p=tally.find(key);
    if(p==tally.end()) { tally[key]=val; order.push_back(key);}
    else (*p).second += val;
}

void print(ostream& out)
{
  out << "Времена выполнения фаз алгоритма:\n"  ;
  BOOST_FOREACH(const string& key, order)
   {

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
                                             start(tbb::tick_count::now()){}
  ~stopwatch(){
  stop=tbb::tick_count::now();
  acc->add(category_,stop-start);

  }
private:
     timekeeper * acc;
     const char * category_;
     tbb::tick_count start, stop;
};





/************************ Temporary files management *************************/

class run_filenames_collection{
    public:
    run_filenames_collection(const string& tmpdirname) :
                                      tmpdir(tmpdirname),
                                      unique_part(bfs::unique_path().string()),
                                      dir_file_path(tmpdir),
                                      rfn_format("BEEF_%04d_%s"),
                                      current_file_num(0)
    {
      dir_file_path /= (boost::format("BEEF_temp_%s") % unique_part).str();

    }

    string add(){

         return path_of_file(current_file_num++).string();
        }

    void save_dir() const
    {
        ofstream f;
        f.exceptions(ios_base::failbit | ios_base::badbit);
        f.open(dir_file_path.string());
        f << *this;
    }

    void get_run_file_names(vector<string>* names) const
    {
       for(int i=0;i<current_file_num; i++){
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

     bfs::path path_of_file(int num) const{
       bfs::path p(tmpdir);
       return p /= (boost::format(rfn_format) %  num % unique_part).str();
     }

};

basic_ostream<char>& operator<<(basic_ostream<char>& os,const run_filenames_collection& rfnc){
  for(int i=0;i<rfnc.current_file_num; i++){
  os << rfnc.path_of_file(i).string() <<"\n";
  }

  return  os;
}



/******************************* raw_ios library *****************************/
// I/O adapters allow perform differently formatted I/O operations on C++ iostreams for the underlying type
// Especially useful for built-in types, when inserter and extracter templates are already defined by the
// standard library .
//
// in our program we need "raw" input and output for THE_KEY_t: read 4 bytes from the stream and
// reinterpret them as a 32-bit integer. And vice versa.
//
template <typename T>
union raw_ios_adapter
{
    T underlying_value;
    raw_ios_adapter(){}//default constructor does nothing
    raw_ios_adapter(const T& src){underlying_value=src;} // conversion from underlying type
    operator T() const { return underlying_value;} //conversion to underlying type

};



template<typename T>
basic_istream<char>&
operator>> (basic_istream<char>& is,
            raw_ios_adapter<T>& x){
 streamsize size   = sizeof(T)/sizeof(char);
 streamsize actual = is.rdbuf()->sgetn(reinterpret_cast<char*>(&x.underlying_value),size);
 if(actual==size) return is;
 is.setstate(ios::eofbit | ios::failbit); //yes, we're at eof. and, no, the target has not been read!

 return is;
}


template<typename T>
basic_ostream<char>&
operator<< (basic_ostream<char>& os,
            const raw_ios_adapter<T>& x){
 streamsize size   = sizeof(T)/sizeof(char);
 streamsize actual = os.rdbuf()->sputn(reinterpret_cast< const char*>(&x.underlying_value), size);
 if(actual!=size) {
     os.setstate( ios::failbit);
     if(os.exceptions() & ios::failbit) throw ios_base::failure("Ошибка при записи в файл");
 }
 return os;
}

/*
// Sorry, GCC 4.5 does not support template aliases  and we
// cannot use more elegant type names  as raw_istream_iterator<THE_KEY_t>

   template <typename Underlying>
   using raw_istream_iterator=istream_iterator<raw_ios_adapter<Underlying> >;

   template <typename Underlying>
   using raw_ostream_iterator=ostream_iterator<raw_ios_adapter<Underlying> >;

*/
// actually we need just a couple of instantiations for our program

typedef istream_iterator<raw_ios_adapter<THE_KEY_t> > raw_istream_iterator_on_KEY_;
typedef ostream_iterator<raw_ios_adapter<THE_KEY_t> > raw_ostream_iterator_on_KEY_;




/******************************** merger class *******************************/

//
// class multi_file_merger
// gets a vector of temporary file names in the constructor
// preconditions:
//            1) there is at least two temporary file names in the input vector
//            2) all temporary files do exist, are readable and are sorted
//
// outputs data to the output file
//
// operates in two modes:
// 1) multi-merge
// 2) 2-merge (with std::merge STL algorithm)
//
// multi-merge mode is implemented with heap data structure (on top of C++ vector
//                                                  and xxxxx_heap STL algorithms
//
// In multi-merge mode the following invariant 'invariant3' is maintained
//  - there are at least 3 elements on the heap
//  - heap does not contain exhausted (eof) iterators
//  - member variable 'runner_up' equals to the minimum of the _next_ values
//     of second and third elements of the heap (heap_vec[1] and heap_vec[2])
//  - the heap satisfies the heap condition
//
//  when the 'invariant3' could not be mantained anymore, the merger swithces
//  to the 2-merge mode

class multi_file_merger {
public:
typedef vector<shared_ptr<ifstream> > ifstream_container;

// istream iterator which remembers which file it operates upon
class extract_iterator: public raw_istream_iterator_on_KEY_{
   public:
       extract_iterator(istream& is, int index): raw_istream_iterator_on_KEY_(is), file_index(index){}
       extract_iterator():raw_istream_iterator_on_KEY_(),file_index(-1){}
       void close_and_delete(ifstream_container& streams, const vector<string> & filenames, bool keep_temporary=false)
       {
           (*streams[file_index]).close();
           if(keep_temporary) return;
           bfs::remove(filenames[file_index]);
       }
   private:
   int file_index; // index to the ifstreams and run_names_ vectors of the enclosing class
};
// We are using  'min' heap where the iterator pointing to the _least_ element is at the top
typedef vector<extract_iterator> heap_type;
class value_is_gt{ // *a > *b comparison we need for the 'min' heap
 public:
 bool operator()(extract_iterator a, extract_iterator b)
 {
     if (a==eof) return true; // exhausted files to the top (to be popped out).
                              // Therefore 'eof' iterator behaves as having the largest value
     if (b==eof) return false;  // -"-
     return static_cast<THE_KEY_t> (*a) >  static_cast<THE_KEY_t>(*b);

 }
};


explicit multi_file_merger(const vector<string>& run_names, bool keep_temporary=false):run_names_(run_names),
                                        ifstreams(),
                                        heap_vec(run_names.size()),
                                        keep_tmp(keep_temporary)
{
  // fill the vector with shared pointers to open input streams
  transform(run_names_.begin(), run_names_.end(),
            back_inserter(ifstreams),
            [](const string& name){
                 shared_ptr<ifstream> shp_ifstream(new ifstream);
                     shp_ifstream->exceptions(ios_base::failbit | ios_base::badbit); // throw exception if open fails
                     shp_ifstream->open(name, ios_base::binary);
                     shp_ifstream->exceptions(ios_base::goodbit); // do not throw exceptions during further operation
                     return shp_ifstream;
                     });
  int count=0;
  transform(ifstreams.begin(),ifstreams.end(),
           heap_vec.begin(),
           [&count](ifstream_container::const_reference shp){
               return extract_iterator(*shp,count++);
           });

  make_heap(heap_vec.begin(),heap_vec.end(),value_is_gt());
  update_runner_up();

}


void operator()(  ofstream& output)
{


    bool invariant_holds=heap_size() > 2;
    while(invariant_holds)
    {
        output << next();
        invariant_holds=restore_invariant3();
    }
    assert(heap_size()==2);
    raw_ostream_iterator_on_KEY_ o(output);
    merge2(o);

}

int heap_size()const {return heap_vec.size();}

private:


raw_ios_adapter<THE_KEY_t> next(){

 return  *heap_vec.front()++;
 }

 bool restore_invariant3() // for our heap-based merge we must have at least 3 sources not exhausted
{
    if(heap_vec.front()==eof){
      pop_heap(heap_vec.begin(),heap_vec.end(),value_is_gt());
      heap_vec.back().close_and_delete(ifstreams,run_names_, keep_tmp);
      heap_vec.pop_back(); // remove the eof element
      if(heap_vec.size()< 3) return false;
      update_runner_up();
    }
    if(static_cast<THE_KEY_t>(*heap_vec.front()) > runner_up) {
      pop_heap(heap_vec.begin(),heap_vec.end(),value_is_gt());
      push_heap(heap_vec.begin(),heap_vec.end(),value_is_gt());
      update_runner_up();
    }

    return true; //we have 3 elements or more, top is not eof and points to the least,
}


 // merge the remaining two streams till the end
 void merge2(raw_ostream_iterator_on_KEY_& o){

      merge(heap_vec[0],eof,heap_vec[1],eof,o);
      heap_vec[0].close_and_delete(ifstreams,run_names_,keep_tmp);
      heap_vec[1].close_and_delete(ifstreams,run_names_,keep_tmp);



 }


private:
void update_runner_up()
 {
     extract_iterator p1=heap_vec[1],p2=heap_vec[2];
     runner_up =value_is_gt()(p1,p2)? *p2 : *p1;
 }


private:
  static const extract_iterator eof;
  vector<string> run_names_;
  ifstream_container ifstreams;
  heap_type heap_vec;
  const bool keep_tmp;
  THE_KEY_t runner_up;

};

const multi_file_merger::extract_iterator multi_file_merger::eof;





/****************** Algorithm stages and helper functions ********************/

void write_output_file(char * const buffer,streamsize n,const string& filename){
        ofstream d;
        d.exceptions(ios_base::failbit | ios_base::badbit); // throw exception on write error (e.g. disk full)
        d.open(filename, ios_base::binary);
        if(n) d.write(buffer,n);
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
                        timekeeper& tk)
{
        size_t io_buffer_size=memory_use;
        vector<THE_KEY_t> buffer_container(io_buffer_size/BYTES_IN_KEY);
        io_buffer_size=buffer_container.size() * BYTES_IN_KEY; // round down to the key size
                                                               //
        char * const input_buffer= reinterpret_cast<char * const>(& buffer_container.front());

        ifstream f;
        f.open(input_file_name, ios_base::binary);

        bool first_iteration=true; // is it the first time the loop being executed ?
        while(f){
         streamsize n=0;
         {

           stopwatch tmp_timer(&tk,"stage1 read");
           f.read(input_buffer,io_buffer_size);
           n=f.gcount();
           f.peek(); // set eof flag to test on the next iteration
         }
         {
         stopwatch tmp_timer(&tk,"parallel_sort");

         tbb::parallel_sort(buffer_container.begin(),buffer_container.begin() + n/BYTES_IN_KEY );
         }

         //  NB fancy static_cast below to stop compiler warning while preserving intention
         //  size_t is unsigned and streamsize is signed
         if(first_iteration &&
            (static_cast<intmax_t>(n) < static_cast<intmax_t>(io_buffer_size) || f.peek()==EOF)) {
/*          cerr << "n=" <<  static_cast<intmax_t>(n)
               << ", buffer_size=" << static_cast<intmax_t>(io_buffer_size)
               <<  ", peek==EOF:" <<(f.peek()==EOF) <<endl;*/
          // The input file has fitted in our buffer!
          // Therefore no temporary files are necessary.
          // Write directly to the output file and exit
          f.close(); // output file could be the same as input;
                     // closing the input to be sure everything goes ok
          stopwatch tmp_timer(&tk,"write output");
          write_output_file(input_buffer,n,output_file_name);
          return false;
         }


        ofstream s;
        s.exceptions(ios_base::failbit | ios_base::badbit); //  we should catch "disk full" condition
        s.open(
               tmpfilenames.add(), // creating another run file
               ios_base::binary );

        stopwatch tmp_timer(&tk,"write tmp files");
        s.write(input_buffer,n);
        first_iteration=false;
        }
        // save all the temporary file names to the 'dir' file for the merge
        // utility to find them later

    return true;
}

void merge_sorted_runs(const vector<string>& run_names, const string& ofilename, bool keep_temporary){

  assert(run_names.size() > 0);  // zero temporary files case should have been
                                 // already dealt with!
  if(run_names.size() < 2)
      {

          cerr << "Предупреждение: имеется всего один временный файл, следовательно\n"
                  "первая стадия алгоритма сработала неоптимально.\n\n"
                  "Выполняется простое копирование ..." <<endl;

           bfs::copy_file(run_names.front(), ofilename, bfs::copy_option::overwrite_if_exists);
           if(!keep_temporary) bfs::remove(run_names.front());
           return ;
      }


    ofstream out;
    out.exceptions(ios_base::failbit | ios_base::badbit);
    out.open(ofilename, ios::binary);
    multi_file_merger merger(run_names, keep_temporary);
    merger(out); // run the algorithm via operator();


}



int main(int argc, char * argv[])
{
    my_options options=read_and_validate_program_options(argc,argv);

    timekeeper tk(options.print_timings ? &cerr : 0);
    stopwatch total_time(&tk,"TOTAL");

    run_filenames_collection tmpfilenames(options.tmpdir);


    try
    {
        if(options.zero_length_input)
        {
            // write zero length output file
            stopwatch tmp_timer(&tk,"write zero-length output");
            write_output_file(0,0, options.output_file);
            return 0;
        }


       bool tempfiles_created=create_sorted_runs(options.max_memory_use,
                                                 options.input_file,
                                                 options.output_file,
                                                 tmpfilenames,
                                                 tk);
      if(!tempfiles_created) return 0;
      if(options.keep_temporary) tmpfilenames.save_dir();
      vector<string> run_names;

      tmpfilenames.get_run_file_names(& run_names);


      //merge temporary files
      stopwatch tmp_timer(&tk,"merge tmp files");
      merge_sorted_runs(run_names,options.output_file, options.keep_temporary);

    }
    catch(bfs::filesystem_error& e)
    {
        cerr<<"Ошибка работы с файлами\n\n";
        cerr<<e.what() <<endl;
        return 3;
    }
    catch(ios_base::failure& e)
    {
        cerr << "Ошибка ввода-вывода: ";
        cerr <<e.what() <<"\n";
        cerr << "Не хватает места на диске либо системный сбой"<<endl;
        return 4;

    }
    catch(std::bad_alloc)
    {
        cerr<<"Ошибка: не удалось удовлетворить запрос на выделение динамической памяти!\n" <<endl;
        return 4;
    }
    catch(tbb::captured_exception& e)
    {
        cerr<<"Ошибка работы с ресурсами.\n"
              "  Библиотека TBB перехватила исключение в фоновом потоке исполнения.\n\n"
              "  Дополнительная информация: " << e.what() <<endl;
        return 5;
    }
    catch(tbb_exception& e)
    {
       cerr<<"Ошибка: исключение в библиотеке TBB \n\n";
       cerr<<e.what() <<endl;
       return 6;
    }
    return 0;
}
