//
// Created by 杜建璋 on 25-8-18.
//

/*
 * SELECT S.ID
 * FROM (
 *     SELECT ID, MIN(CS) as cs1, MAX(CS) as cs2
 *     FROM R WHERE R.year=2019 GROUP-BY ID
 *     ) as S
 * WHERE S.cs2 - S.cs1 > c
 */
int main(int argc, char *argv[]) {

}
