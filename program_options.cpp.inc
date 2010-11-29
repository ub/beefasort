class my_options_parser_and_validator {

     friend my_options read_and_validate_program_options(int ac, char* av[]);
     my_options_parser_and_validator(int ac, char* av[],my_options & result):
          desc( "Опции:"),
          argv0(av[0]),
          vm(),
          result(&result) {
          desc.add_options()
          ("help",                                 "Вывести это сообщение")
          ("input-file",
           po::value<string>(&result.input_file),  "файл на входе")
          ("output-file",
           po::value<string>(&result.output_file), "файл на выходе")
          ("tmp-dir,t",
           po::value<string>(&result.tmpdir),      "директория для создания временных файлов")
          ("force,f",
           po::value<bool>(&result.force_output_overwrite)->implicit_value (true)
                                                          ->default_value  (false),
                                                   "заменить существующий выходной файл")
          ("show-time-stats,s",
           po::value<bool>(&result.print_timings)         ->implicit_value (true)
                                                          ->default_value  (false),
                                            "показать время выполнения разных фаз алгоритма")
          ("keep-tmpfiles,k",
           po::value<bool>(&result.keep_temporary)        ->implicit_value (true)
                                                          ->default_value  (false),
                                                "не удалять временные файлы после завершения")
          ("max-memory-use,M",
            po::value<int>()->default_value(DEFAULT_MAX_MEMUSE_IN_MB),
           (boost::format("максимальный размер выделяемой памяти в мегабайтах 1 - %d") %
                           REASONABLE_MEMORY_LIMIT_IN_MB).str().c_str() )
          ;

          po::positional_options_description p;
          p.add("input-file", 1);
          p.add("output-file", 1);

          po::store(po::command_line_parser(ac, av).
                    options(desc).positional(p).run(), vm);
          po::notify(vm);
          if(vm.count("help")) show_usage_and_exit(0);
          if(ac < 3) {
               cerr << "Ошибка: недостаточно параметров! \n\n";
               show_usage_and_exit(1);
          }
     }



     void show_usage_and_exit(int exit_code) {
          cerr << "Использование: " << argv0 << " <файл на входе> <файл на выходе> [опции]\n";
          cerr << desc <<endl;
          exit(exit_code);
     }

     void check_input_file() {
          if (vm.count("input-file")) {
               bfs::path input_file( vm["input-file"].as< string >()) ;
               if(! bfs::exists(input_file)) {
                    cerr <<"Ошибка: входной файл "<<input_file<<" не существует!\n\n";
                    show_usage_and_exit(2);
               }

               try {
                    uintmax_t  file_size = bfs::file_size(input_file);
                    if( file_size ) {
                         ifstream test;
                         test.exceptions(ios_base::failbit | ios_base::badbit | ios_base::eofbit );
                         test.open(input_file.string(), ios::binary);
                         test.peek();
                         int remainder = file_size % 4;
                         if(remainder!=0){
                             cerr <<"Предупреждение: размер входного файла "<<input_file<<" не кратен четырем!\n\n";
                             switch(remainder){
                                 case 1:
                                     cerr << "    (последний байт файла будет отброшен)" <<endl;
                                 break;
                                 default:
                                     cerr << "    (последние " << remainder  << " байта файла будут отброшены)" << endl;
                             }

                         }
                    } else result->zero_length_input=true;
               } catch(ios_base::failure& e) {
                    cerr <<"Ошибка: не удается прочесть входной файл "<<input_file<<"!\n\n";
                    show_usage_and_exit(3);
               } catch(bfs::filesystem_error& e) {
                    cerr<<"Ошибка: недопустимый входной файл "<<input_file<<" (не удается определить размер)\n\n";
                    cerr<<e.what() <<endl;
                    exit(3);

               }
          } else {
               cerr<<"Ошибка: имя входного файла не задано!\n\n";
               show_usage_and_exit(1);
          }
     }
     void check_output_file() {
          if (vm.count("output-file")) {
               bfs::path output_file(vm["output-file"].as< string >());
               ios::openmode outfile_openmode_for_test= ios::out | ios::binary;
               bool should_delete_after_test=true;
               if (bfs::exists(output_file)) {
                    outfile_openmode_for_test |= ios::in;
                    should_delete_after_test=false;
                    if (! vm["force"].as<bool>()) {
                         cerr <<"Предупреждение: выходной файл существует!\n"
                              "Используйте опцию -f, чтобы переписать существующий файл.\n\n";
                         show_usage_and_exit(2);
                    }
               }
               try {
                    ofstream test;
                    test.exceptions(ios_base::failbit | ios_base::badbit );
                    test.open(output_file.string(),  outfile_openmode_for_test);
                    test.close();
                    if(should_delete_after_test)bfs::remove(output_file);
               } catch(ios_base::failure& e) {
                    cerr <<"Ошибка: не удается открыть/создать выходной файл "<<output_file<<"!\n\n";
                    show_usage_and_exit(3);
               }
          } else {
               cerr<<"Ошибка: имя выходного файла не задано!\n\n";
               show_usage_and_exit(1);
          }

     }

     void check_tmp_dir() {
          if (vm.count("tmp-dir")) {
               bfs::path tmpdir( vm["tmp-dir"].as< string >()) ;
               if(!bfs::exists(tmpdir) || !bfs::is_directory(tmpdir)) {
                    cerr<<"Ошибка: указанная временная директория " << tmpdir<< " отсутствует или неприемлема!\n\n";
                    show_usage_and_exit(3);
               }
               try {
                    bfs::path tmp_file_pattern(tmpdir);
                    tmp_file_pattern /= "%%%%-%%%%-%%%%-%%%%";
                    ofstream test;
                    test.exceptions(ios_base::failbit | ios_base::badbit );
                    bfs::path test_file_path(unique_path(tmp_file_pattern));
                    test.open(test_file_path.string(), ios::binary );
                    test.close();
                    bfs::remove(test_file_path);
               } catch(ios_base::failure& e) {
                    cerr <<"Ошибка: не удается создать временный файл  в директории "<<tmpdir<<"!\n\n";
                    show_usage_and_exit(3);
               }

          }

     }

     void check_memory_use() {
          if (vm.count("max-memory-use")) {
               int mb=vm["max-memory-use"].as<int>();
               if (mb < 1 || mb > REASONABLE_MEMORY_LIMIT_IN_MB) {
                    cerr<<"Ошибка: запрошен недопустимый лимит памяти (" << mb<<"M)!\n\n";
                    exit(1);
               }
               result->max_memory_use=static_cast<size_t>(mb) <<20;
          }
     }

     void  validate() {
          check_input_file();
          check_output_file();
          check_tmp_dir();
          check_memory_use();
     }

     po::options_description desc;
     const char * argv0;
     po::variables_map vm;
     my_options * result;

};



my_options read_and_validate_program_options(int ac, char* av[])
{
     my_options result;
     try {

          my_options_parser_and_validator options_processor(ac,av, result);
          options_processor.validate();


          size_t approved=validate_memory_reqs(result.max_memory_use, DEFAULT_MAX_MEMUSE);
          if(approved) result.max_memory_use=approved;
          else {
               cerr <<"Ошибка: не хватает свободной памяти!\n\n";
               exit(3);
          }

     } catch(bfs::filesystem_error& e) {
          cerr<<"Ошибка тестирования файлов \n\n";
          cerr<<e.what() <<endl;
          exit(3);

     } catch(po::error& e) {
          cerr<<"Ошибка при разборе командной строки\n\n";
          cerr<<e.what() <<endl;
          exit(3);
     }

     return result;

}


